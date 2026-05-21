# =========================================================
# SCOPEAI ACTOR v8.0 - THIN ENQUEUE ACTOR
# =========================================================
# Architecture: discover recruiters → enqueue ONLY
# Backend worker handles ALL sending, SMTP rotation, retries
# =========================================================

import asyncio
import os
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlparse

import httpx
from apify import Actor
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from .outbound_queue import enqueue_emails_for_job
from .prospeo_client import ProspeoClient
from .recruiter_discovery import RecruiterDiscovery
from .redis_cache import RedisCache

# =========================================================
# URL VALIDATION
# =========================================================
def normalize_api_base(api_base: str) -> str:
    if not api_base:
        return ""
    api_base = api_base.rstrip('/')
    if "localhost" in api_base.lower() or "127.0.0.1" in api_base:
        return ""
    if not api_base.startswith(('http://', 'https://')):
        api_base = f"https://{api_base}"
    try:
        parsed = urlparse(api_base)
        if not parsed.netloc:
            return ""
        return api_base
    except Exception:
        return ""

# =========================================================
# JOB RESULT TRACKING
# =========================================================
class JobResult:
    def __init__(self, job_id: str, job_title: str, company_name: str):
        self.job_id = job_id
        self.job_title = job_title
        self.company_name = company_name
        self.status = "pending"
        self.error_message = None
        self.application_url = None
        self.applied_at = None
        self.emails_enqueued = 0

    def mark_success(self, emails_enqueued: int = 1, application_url: str = None):
        self.status = "success"
        self.emails_enqueued = emails_enqueued
        self.applied_at = datetime.now(timezone.utc).isoformat()
        if application_url:
            self.application_url = application_url

    def mark_failed(self, error_message: str = None):
        self.status = "failed"
        self.error_message = error_message
        self.applied_at = datetime.now(timezone.utc).isoformat()

    def mark_skipped(self, reason: str = None):
        self.status = "skipped"
        self.error_message = reason
        self.applied_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict:
        return {
            "job_id":           self.job_id,
            "job_title":        self.job_title,
            "company_name":     self.company_name,
            "status":           self.status,
            "error_message":    self.error_message,
            "application_url":  self.application_url,
            "applied_at":       self.applied_at,
        }

# =========================================================
# SSE LOGGING
# =========================================================
async def log_to_backend(run_id: str, user_id: str, api_base: str, message: dict):
    try:
        if not api_base:
            return
        message["run_id"] = run_id
        message["user_id"] = user_id
        message["ts"] = datetime.now(timezone.utc).isoformat()
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{api_base}/v1/automation/sse-log", json=message)
    except Exception:
        pass

# =========================================================
# CALLBACK
# =========================================================
async def send_callback_to_backend(
    run_id: str,
    user_id: str,
    api_base: str,
    status: str,
    total_jobs: int,
    job_results: List[JobResult],
    error_message: str = None,
) -> bool:
    if not api_base:
        print("[Callback] ⚠️ No valid API_BASE for callback")
        return False

    # "success" here means emails were enqueued for async delivery, not confirmed sent
    successful            = sum(1 for jr in job_results if jr.status == "success")
    skipped_dedup         = sum(1 for jr in job_results if jr.status == "skipped")
    failed_actual         = sum(1 for jr in job_results if jr.status == "failed")
    total_emails_enqueued = sum(jr.emails_enqueued for jr in job_results)

    payload = {
        "run_id":                    run_id,
        "user_id":                   user_id,
        "status":                    status,
        "total_jobs":                total_jobs,
        "successful_applications":   successful,          # jobs where ≥1 email entered the queue
        "failed_applications":       failed_actual,       # genuine failures only (not dedup skips)
        "already_contacted":         skipped_dedup,       # dedup hits — NOT failures
        "emails_enqueued":           total_emails_enqueued,
        "jobs_with_emails_enqueued": successful,
        "job_results":               [jr.to_dict() for jr in job_results],
        "error_message":             error_message,
        "completed_at":              datetime.now(timezone.utc).isoformat(),
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{api_base}/v1/automation/callback",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            if response.status_code == 200:
                print(f"[Callback] ✅ Sent: {status}")
                return True
            print(f"[Callback] ❌ HTTP {response.status_code}")
    except Exception as e:
        print(f"[Callback] ❌ {str(e)[:80]}")

    return False

# =========================================================
# FINISH RUN
# =========================================================
async def finish_automation_run(
    run_id: str,
    job_results: List[JobResult],
    status: str,
    started: datetime,
    api_base: str,
    stats: Dict,
    user_id: str,
    failure_reason: str = None,
):
    try:
        duration = max(1, int((datetime.utcnow() - started).total_seconds()))

        if status == "failed":
            await log_to_backend(run_id, user_id, api_base, {
                "type": "error",
                "msg": f"❌ Automation failed: {failure_reason or 'Unknown error'}",
                "duration": duration,
            })
        else:
            await log_to_backend(run_id, user_id, api_base, {
                "type":                  "completion",
                "msg":                   "🎉 Automation completed! ⚡ Worker will process asynchronously",
                "run_id":                run_id,
                "user_id":               user_id,
                "jobs_processed":        stats.get("processed", 0),
                "recruiters_discovered": stats.get("recruiters_discovered", 0),
                "emails_enqueued":       stats.get("emails_enqueued", 0),
                "cache_hits":            stats.get("cache_hits", 0),
                "prospeo_hits":          stats.get("prospeo_hits", 0),
                "hunter_fallbacks":      stats.get("hunter_fallbacks", 0),
                "ai_fallbacks":          stats.get("ai_fallbacks", 0),
                "duration":              duration,
                "ts":                    datetime.now(timezone.utc).isoformat(),
            })

        await send_callback_to_backend(
            run_id, user_id, api_base, status, len(job_results), job_results, failure_reason
        )
    except Exception as e:
        print(f"[Finish] ⚠️ {e}")

# =========================================================
# MAIN ACTOR
# =========================================================
async def run_actor():
    async with Actor() as actor:
        input_data = await actor.get_input() or {}

        api_base    = normalize_api_base(input_data.get("API_BASE") or os.getenv("API_BASE", ""))
        mistral_key = input_data.get("MISTRAL_API_KEY") or os.getenv("MISTRAL_API_KEY", "")
        openai_key  = input_data.get("OPENAI_API_KEY")  or os.getenv("OPENAI_API_KEY", "")
        hunter_key  = input_data.get("HUNTER_API_KEY")  or os.getenv("HUNTER_API_KEY", "")
        prospeo_key = input_data.get("PROSPEO_API_KEY") or os.getenv("PROSPEO_API_KEY", "")
        redis_url   = (
            input_data.get("REDIS_PUBLIC_URL") or os.getenv("REDIS_PUBLIC_URL", "")
            or input_data.get("REDIS_URL")      or os.getenv("REDIS_URL", "")
        )

        run_id      = input_data.get("run_id")
        user_id     = input_data.get("user_id")
        jobs        = input_data.get("jobs", [])
        cv_json     = input_data.get("cv_json", {})
        cv_file_url = input_data.get("cv_file_url", "")
        max_jobs    = int(input_data.get("max_jobs", 10))

        started      = datetime.utcnow()
        job_results: List[JobResult] = []
        stats_lock   = asyncio.Lock()

        stats = {
            "total_jobs":            len(jobs),
            "processed":             0,
            "recruiters_discovered": 0,
            "emails_enqueued":       0,
            "cache_hits":            0,
            "prospeo_hits":          0,
            "hunter_fallbacks":      0,
            "scraping_hits":         0,
            "ai_fallbacks":          0,
            "applications_success":  0,
            "applications_failed":   0,
            "already_contacted":     0,   # dedup hits — NOT failures
        }

        actor.log.info("=" * 60)
        actor.log.info("🚀 SCOPEAI ACTOR v8.0 - THIN ENQUEUE ACTOR")
        actor.log.info("=" * 60)
        actor.log.info(f"Run ID: {run_id} | User ID: {user_id} | Jobs: {len(jobs)}")
        actor.log.info(f"API Base: {api_base}")
        actor.log.info("=" * 60)

        failure_reason = None

        try:
            if not run_id or not user_id:
                raise ValueError("Missing required run_id or user_id")

            if not jobs:
                actor.log.warning("⚠️ No jobs provided")
                await finish_automation_run(
                    run_id, job_results, "failed", started, api_base, stats, user_id, "No jobs provided"
                )
                return

            if not cv_json:
                raise ValueError("CV data is missing or invalid")

            await log_to_backend(run_id, user_id, api_base, {
                "type": "info",
                "msg": f"🚀 Actor started processing {len(jobs)} jobs",
                "total_jobs": len(jobs),
            })

            jobs = jobs[:max_jobs]

            for idx, job in enumerate(jobs):
                job_id = job.get("id") or job.get("job_id") or f"job_{idx}"
                job["id"] = job_id
                jr = JobResult(str(job_id), job.get("title", "Unknown Title"), job.get("company", "Unknown Company"))
                job_results.append(jr)
                actor.log.info(f"[Init] Job {idx}: ID={job_id} | {job.get('title')} @ {job.get('company')}")

            cache = RedisCache(redis_url)
            await cache.connect()

            prospeo = ProspeoClient(prospeo_key)

            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                )

                discovery = RecruiterDiscovery(
                    browser=browser,
                    prospeo=prospeo,
                    cache=cache,
                    hunter_key=hunter_key,
                    mistral_key=mistral_key,
                    openai_key=openai_key,
                )

                sem = asyncio.Semaphore(3)

                async def process_job(job_idx: int, job: Dict, job_result: JobResult):
                    async with sem:
                        title       = job.get("title", "")
                        url         = job.get("url") or job.get("link", "")
                        company     = job.get("company", "")
                        description = job.get("description") or job.get("snippet", "")

                        actor.log.info(f"[{job_idx}/{len(jobs)}] 📋 {title} @ {company}")

                        if not title or not company or not description:
                            job_result.mark_skipped("Missing title, company, or description")
                            async with stats_lock:
                                stats["applications_failed"] += 1
                            return

                        try:
                            await log_to_backend(run_id, user_id, api_base, {
                                "type":     "info",
                                "msg":      f"🔍 Processing {job_idx}/{len(jobs)}: {title} at {company}",
                                "job_title": title,
                                "company":  company,
                                "progress": f"{job_idx}/{len(jobs)}",
                            })

                            result = await discovery.discover(description, title, company, url)

                            if not result["success"] or not result["emails"]:
                                actor.log.warning(f"⚠️ No emails found: {company}")
                                job_result.mark_failed("No valid emails discovered")
                                async with stats_lock:
                                    stats["applications_failed"] += 1
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type":     "warning",
                                    "msg":      f"⚠️ No emails found for {company}",
                                    "job_title": title,
                                    "company":  company,
                                })
                                return

                            emails       = result["emails"]
                            company_info = result["company_info"]
                            method       = result["method"]
                            cache_hit    = result.get("cache_hit", False)
                            prospeo_stats = result.get("prospeo_stats", {})

                            job_result.company_name = company_info["company_name"]

                            async with stats_lock:
                                stats["recruiters_discovered"] += 1
                                if cache_hit:
                                    stats["cache_hits"] += 1
                                elif method == "prospeo":
                                    stats["prospeo_hits"] += 1
                                    stats["cache_hits"] += prospeo_stats.get("cache_hits", 0)
                                elif method == "hunter":
                                    stats["hunter_fallbacks"] += 1
                                elif method == "scraped":
                                    stats["scraping_hits"] += 1
                                elif method == "generated":
                                    stats["ai_fallbacks"] += 1

                            if cache_hit:
                                actor.log.info(f"🔁 Cached recruiter: {company_info['company_name']}")
                            else:
                                actor.log.info(
                                    f"✅ {len(emails)} emails via {method.upper()}: {company_info['company_name']}"
                                )

                            await log_to_backend(run_id, user_id, api_base, {
                                "type": "info",
                                "msg": (
                                    f"🔁 Using cached recruiter for {company_info['company_name']}"
                                    if cache_hit else
                                    f"✅ Found {len(emails)} emails via {method} for {company_info['company_name']}"
                                ),
                                "job_title":    title,
                                "company":      company_info["company_name"],
                                "emails_found": len(emails),
                                "method":       method,
                            })

                            enqueued, skip_reason = await enqueue_emails_for_job(
                                run_id, user_id, job, emails, cv_json, cv_file_url,
                                api_base, cache, email_source=method,
                            )

                            if skip_reason == "already_contacted":
                                # Dedup worked correctly — this is NOT a failure
                                job_result.mark_skipped("Company already contacted in a previous run")
                                async with stats_lock:
                                    stats["already_contacted"] += 1
                                    stats["processed"]         += 1
                                actor.log.info(f"⏭️ Already contacted: {company_info['company_name']} — skipped")
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type":    "info",
                                    "msg":     f"⏭️ Already contacted {company_info['company_name']} in a previous run — skipped (dedup)",
                                    "job_title": title,
                                    "company": company_info["company_name"],
                                })
                            elif enqueued > 0:
                                job_result.mark_success(emails_enqueued=enqueued, application_url=url)
                                async with stats_lock:
                                    stats["emails_enqueued"]      += enqueued
                                    stats["applications_success"] += 1
                                    stats["processed"]            += 1
                                actor.log.info(
                                    f"📥 Enqueued {enqueued}/{len(emails)} emails for {company_info['company_name']} "
                                    f"⚡ Worker will process asynchronously"
                                )
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type":            "success",
                                    "msg":             (
                                        f"📥 Enqueued {enqueued}/{len(emails)} emails for "
                                        f"{company_info['company_name']} ⚡ Worker will process asynchronously"
                                    ),
                                    "job_title":       title,
                                    "company":         company_info["company_name"],
                                    "emails_enqueued": enqueued,
                                })
                            else:
                                job_result.mark_failed("Enqueue request failed")
                                async with stats_lock:
                                    stats["applications_failed"] += 1
                                    stats["processed"]           += 1
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type":    "warning",
                                    "msg":     f"❌ Enqueue failed for {company_info['company_name']}",
                                    "job_title": title,
                                    "company": company_info["company_name"],
                                })

                        except Exception as e:
                            actor.log.error(f"❌ Job error [{title}]: {str(e)[:100]}")
                            actor.log.error(traceback.format_exc())
                            job_result.mark_failed(f"Processing error: {str(e)[:100]}")
                            async with stats_lock:
                                stats["applications_failed"] += 1
                            await log_to_backend(run_id, user_id, api_base, {
                                "type":     "error",
                                "msg":      f"❌ Error processing {title}: {str(e)[:100]}",
                                "job_title": title,
                                "company":  company,
                            })

                tasks = [
                    process_job(idx + 1, job, jr)
                    for idx, (job, jr) in enumerate(zip(jobs, job_results))
                ]
                await asyncio.gather(*tasks)
                await browser.close()

            await cache.close()

            successful        = sum(1 for jr in job_results if jr.status == "success")
            already_contacted = stats.get("already_contacted", 0)
            # Only count genuine failures — not dedup skips
            failed = sum(
                1 for jr in job_results
                if jr.status == "failed"
                # "skipped" with dedup reason is NOT a failure
                or (jr.status == "skipped" and jr.error_message != "Company already contacted in a previous run")
            )

            if successful > 0 and failed > 0:
                final_status = "partial"
            elif successful > 0:
                final_status = "completed"
            elif already_contacted > 0 and failed == 0:
                # All jobs were dedup-skipped — deduplication worked correctly
                final_status = "completed"
            else:
                final_status = "failed"

            await finish_automation_run(
                run_id, job_results, final_status, started, api_base, stats, user_id
            )

            actor.log.info("=" * 60)
            actor.log.info("🎉 AUTOMATION COMPLETED!")
            actor.log.info("=" * 60)
            actor.log.info(f"📊 Jobs Processed:        {stats['processed']}/{stats['total_jobs']}")
            actor.log.info(f"🔍 Recruiters Discovered: {stats['recruiters_discovered']}")
            actor.log.info(f"📥 Emails Enqueued:       {stats['emails_enqueued']}")
            actor.log.info(f"💾 Cache Hits:            {stats['cache_hits']}")
            actor.log.info(f"⚡ Prospeo Hits:          {stats['prospeo_hits']}")
            actor.log.info(f"🔄 Hunter Fallbacks:      {stats['hunter_fallbacks']}")
            actor.log.info(f"🌐 Scraping Hits:         {stats['scraping_hits']}")
            actor.log.info(f"🤖 AI Fallbacks:          {stats['ai_fallbacks']}")
            actor.log.info(f"📥 Jobs w/ Emails Queued: {stats['applications_success']} (async delivery — not confirmed sent)")
            actor.log.info(f"❌ Jobs Failed/Skipped:   {stats['applications_failed']}")
            actor.log.info(f"⏱️  Duration:              {int((datetime.utcnow() - started).total_seconds())}s")
            actor.log.info("=" * 60)

        except Exception as e:
            failure_reason = str(e)
            actor.log.error(f"💥 FATAL ERROR: {e}")
            actor.log.error(traceback.format_exc())
            await finish_automation_run(
                run_id, job_results, "failed", started, api_base, stats, user_id, failure_reason
            )


def main():
    asyncio.run(run_actor())


if __name__ == "__main__":
    main()
