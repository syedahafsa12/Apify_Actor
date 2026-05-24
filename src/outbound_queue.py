import asyncio
import re
from typing import Dict, List

import httpx

from .redis_cache import RedisCache, TTL_7DAYS

_run_enqueued_emails: dict[str, set] = {}

_CLOSING_PREFIXES = frozenset({
    "best regards", "kind regards", "sincerely", "regards",
    "cv attached", "one attachment", "scanned by",
})


def _strip_summary_boilerplate(text: str, name: str, email: str) -> str:
    """Remove greeting lines and trailing signature block from a pre-written summary."""
    lines = text.strip().splitlines()

    # Drop leading blank lines and any "Dear ..." greeting
    while lines and (not lines[0].strip() or re.match(r"^dear\b", lines[0].strip(), re.IGNORECASE)):
        lines.pop(0)

    # Drop trailing blank lines, then trailing signature / closing lines
    name_lc  = name.lower()
    email_lc = email.lower()
    while lines:
        last = lines[-1].strip()
        ll   = last.lower()
        if (
            not last
            or any(ll.startswith(p) for p in _CLOSING_PREFIXES)
            or (name_lc  and name_lc  in ll)
            or (email_lc and email_lc in ll)
            or re.match(r"^\+?\d[\d\s\-\(\)]{6,}$", last)   # phone number
        ):
            lines.pop()
        else:
            break

    return "\n".join(lines).strip()


def _build_cover_letter(cv_json: Dict, job_title: str, company: str) -> str:
    name    = cv_json.get("name", "Job Applicant")
    summary = cv_json.get("summary", "")
    skills  = cv_json.get("skills", [])
    email   = cv_json.get("email") or cv_json.get("contact", {}).get("email", "")
    phone   = cv_json.get("phone") or cv_json.get("contact", {}).get("phone", "") or ""

    greeting = f"Dear {company} Hiring Team," if company else "Dear Hiring Team,"
    body = f"{greeting}\n\n"

    if summary:
        cleaned = _strip_summary_boilerplate(summary, name, email)
        if cleaned:
            body += f"{cleaned}\n\n"
    else:
        body += f"I am writing to express my interest in the {job_title} position at {company}.\n\n"

    if skills:
        body += f"Key skills: {', '.join(str(s) for s in skills[:8])}.\n\n"

    body += f"I have attached my CV for your review.\n\nBest regards,\n{name}"
    if email:
        body += f"\n{email}"
    if phone:
        body += f"\n{phone}"
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

    # In-batch dedup: prevent same email being sent twice in one run
    run_set = _run_enqueued_emails.setdefault(run_id, set())
    if to_email in run_set:
        print(f"[Enqueue] ⏭️ Skipping duplicate in current batch: {to_email}")
        return False
    run_set.add(to_email)

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
) -> tuple[int, str]:
    """Enqueue all emails for a job with company-level cross-run dedup.
    Returns (count_queued, skip_reason).
    skip_reason is 'already_contacted' when dedup fired, '' otherwise.
    """
    company = job.get("company", "")
    # Scope per user so one user's history never blocks another user's run
    company_key = f"recruiter_company:{user_id}:{company.lower().replace(' ', '_')[:60]}"

    if await cache.exists(company_key):
        print(f"[Enqueue] ⏭️ Company already contacted: {company}")
        return 0, "already_contacted"

    queued = 0
    for email in emails:
        ok = await enqueue_email(
            run_id, user_id, job, email, cv_json, cv_file_url, api_base, cache, email_source
        )
        if ok:
            queued += 1

    if queued > 0:
        await cache.set(company_key, "1", ttl=TTL_7DAYS)
        # Keep memory bounded: evict oldest run sets beyond last 3
        if len(_run_enqueued_emails) > 3:
            oldest = next(iter(_run_enqueued_emails))
            del _run_enqueued_emails[oldest]

    return queued, ""
