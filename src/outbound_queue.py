import asyncio
from typing import Dict, List

import httpx

from .redis_cache import RedisCache, TTL_7DAYS


def _build_cover_letter(cv_json: Dict, job_title: str, company: str) -> str:
    name = cv_json.get('name', 'Job Applicant')
    summary = cv_json.get('summary', '')
    skills = cv_json.get('skills', [])
    email = cv_json.get('email') or cv_json.get('contact', {}).get('email', '')
    body = (
        f"Dear {company} Hiring Team,\n\n"
        f"I am writing to express my interest in the {job_title} position at {company}.\n\n"
    )
    if summary:
        body += f"{summary}\n\n"
    if skills:
        body += f"Key skills: {', '.join(str(s) for s in skills[:8])}.\n\n"
    body += f"I have attached my CV for your review.\n\nBest regards,\n{name}"
    if email:
        body += f"\n{email}"
    return body


async def enqueue_email(
    run_id: str,
    user_id: str,
    job: Dict,
    to_email: str,
    cv_json: Dict,
    cv_file_url: str,
    api_base: str,
    cache: RedisCache,
    email_source: str = "unknown",
) -> bool:
    """POST one email job to the backend queue. Returns True if accepted."""
    job_id = str(job.get("id") or job.get("job_id") or "")
    job_title = job.get("title", "")
    company = job.get("company", "")
    job_url = job.get("url") or job.get("link", "")

    dedup_key = f"sent:{run_id}:{job_id}:{to_email}"
    if await cache.exists(dedup_key):
        print(f"[Enqueue] ⏭️ Duplicate: {to_email}")
        return False

    recruiter_key = f"recruiter:{to_email}"
    if await cache.exists(recruiter_key):
        print(f"[Enqueue] ⏭️ Recruiter already contacted: {to_email}")
        return False

    applicant_name  = cv_json.get('name', '')
    applicant_email = cv_json.get('email') or cv_json.get('contact', {}).get('email', '')
    applicant_phone = cv_json.get('phone') or cv_json.get('contact', {}).get('phone', '') or ''
    cover_letter    = _build_cover_letter(cv_json, job_title, company)

    payload = {
        "run_id":           run_id,
        "job_url":          job_url,
        "job_title":        job_title,
        "company":          company,
        "to_email":         to_email,
        "cv_file_url":      cv_file_url,
        "cover_letter":     cover_letter,
        "applicant_name":   applicant_name,
        "applicant_email":  applicant_email,
        "applicant_phone":  applicant_phone,
        "ai_discovery":     {"email_source": email_source, "user_id": user_id},
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{api_base}/v1/automation/email-apply",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            if response.status_code in (200, 201, 202):
                await cache.set(dedup_key, "1", ttl=TTL_7DAYS)
                await cache.set(recruiter_key, "1", ttl=TTL_7DAYS)
                print(f"[Enqueue] ✅ Queued: {to_email}")
                return True
            print(f"[Enqueue] ❌ HTTP {response.status_code}: {response.text[:150]}")
    except asyncio.TimeoutError:
        print("[Enqueue] ⏱️ Timeout")
    except Exception as e:
        print(f"[Enqueue] ❌ {str(e)[:80]}")

    return False


async def enqueue_emails_for_job(
    run_id: str,
    user_id: str,
    job: Dict,
    emails: List[str],
    cv_json: Dict,
    cv_file_url: str,
    api_base: str,
    cache: RedisCache,
    email_source: str = "unknown",
) -> int:
    """Enqueue all emails for a job with company-level cross-run dedup.
    Returns count of successfully queued emails."""
    company = job.get("company", "")
    company_key = f"recruiter_company:{company.lower().replace(' ', '_')[:60]}"

    if await cache.exists(company_key):
        print(f"[Enqueue] ⏭️ Company already contacted: {company}")
        return 0

    queued = 0
    for email in emails:
        ok = await enqueue_email(
            run_id, user_id, job, email, cv_json, cv_file_url, api_base, cache, email_source
        )
        if ok:
            queued += 1

    if queued > 0:
        await cache.set(company_key, "1", ttl=TTL_7DAYS)

    return queued
