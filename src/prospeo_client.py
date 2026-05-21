import asyncio
import time
from typing import Dict, List, Optional

import httpx

PROSPEO_BASE = "https://api.prospeo.io"

RECRUITER_TITLES = [
    "Technical Recruiter",
    "Talent Acquisition",
    "HR Manager",
    "Head of People",
    "Recruiter",
    "Engineering Recruiter",
    "Senior Recruiter",
    "Talent Partner",
    "People Operations",
    "HR Business Partner",
    "Hiring Manager",
    "Recruitment Manager",
]


class ProspeoClient:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self.available = bool(api_key)
        self._lock = asyncio.Lock()
        self._last_call = 0.0
        self._window_start = time.monotonic()
        self._window_count = 0

        if self.available:
            print("[Prospeo] ✅ Initialized (Search → Enrich)")
        else:
            print("[Prospeo] ⚠️ No API key - disabled")

    @property
    def _headers(self) -> Dict:
        return {"X-KEY": self.api_key, "Content-Type": "application/json"}

    # ------------------------------------------------------------------ #
    # Rate limiting: 1 req/sec, 30 req/min                                #
    # ------------------------------------------------------------------ #
    async def _rate_limit(self):
        async with self._lock:
            now = time.monotonic()
            if now - self._window_start >= 60:
                self._window_start = now
                self._window_count = 0
            if self._window_count >= 30:
                wait = 60.0 - (now - self._window_start)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._window_start = time.monotonic()
                self._window_count = 0
            elapsed = time.monotonic() - self._last_call
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
            self._last_call = time.monotonic()
            self._window_count += 1

    # ------------------------------------------------------------------ #
    # POST with exponential-backoff retry (max 5 attempts)                #
    # ------------------------------------------------------------------ #
    async def _post(self, endpoint: str, payload: Dict, max_retries: int = 5) -> Optional[Dict]:
        url = f"{PROSPEO_BASE}/{endpoint.lstrip('/')}"
        delay = 2.0

        for attempt in range(1, max_retries + 1):
            await self._rate_limit()
            try:
                async with httpx.AsyncClient(timeout=45) as client:
                    response = await client.post(url, json=payload, headers=self._headers)

                    if response.status_code == 200:
                        data = response.json()
                        if data.get("error"):
                            print(f"[Prospeo] ❌ API error: {data.get('message')}")
                            return None
                        return data

                    if response.status_code == 429:
                        wait = delay * (2 ** (attempt - 1))
                        print(f"[Prospeo] ⚠️ Rate limited - waiting {wait:.0f}s (attempt {attempt}/{max_retries})")
                        await asyncio.sleep(wait)
                        continue

                    if response.status_code == 400:
                        try:
                            err_body = response.json()
                        except Exception:
                            err_body = {}
                        if err_body.get("error_code") == "NO_RESULTS":
                            print(f"[Prospeo] ℹ️ No matches found for {endpoint}. Triggering Hunter.io fallback...")
                            return None
                        print(f"[Prospeo] ❌ HTTP 400 — Bad Request")
                        print(f"[Prospeo]   Endpoint : {url}")
                        print(f"[Prospeo]   Payload  : {payload}")
                        print(f"[Prospeo]   Response : {response.text[:400]}")
                        return None

                    print(f"[Prospeo] ❌ HTTP {response.status_code} on {endpoint}: {response.text[:150]}")
                    return None

            except asyncio.TimeoutError:
                wait = delay * (2 ** (attempt - 1))
                print(f"[Prospeo] ⏱️ Timeout (attempt {attempt}/{max_retries}) - retrying in {wait:.0f}s")
                await asyncio.sleep(wait)
            except Exception as e:
                print(f"[Prospeo] ❌ Request error: {str(e)[:80]}")
                return None

        print(f"[Prospeo] ❌ All {max_retries} retries exhausted for {endpoint}")
        return None

    # ------------------------------------------------------------------ #
    # STEP 1 — Search recruiters at a company domain                      #
    # ------------------------------------------------------------------ #
    async def search_recruiters(self, domain: str, page: int = 1) -> List[Dict]:
        if not self.available:
            return []

        payload = {
            "page": page,
            "filters": {
                "company": {
                    "websites": {
                        "include": [domain]
                    }
                },
                "person_job_title": {
                    "include": RECRUITER_TITLES
                }
            }
        }

        print(f"[Prospeo] 🔍 search-person: {domain} (page {page})")
        data = await self._post("/search-person", payload)
        if not data:
            return []

        persons = (
            data.get("response", {}).get("data")
            or data.get("response", {}).get("results")
            or []
        )
        total = data.get("response", {}).get("total", len(persons))
        print(f"[Prospeo] 📋 {len(persons)}/{total} candidates at {domain}")
        return persons

    # ------------------------------------------------------------------ #
    # STEP 2 — Bulk enrich person_ids (max 50 per batch)                  #
    # ------------------------------------------------------------------ #
    async def bulk_enrich(self, person_ids: List[str]) -> List[Dict]:
        if not self.available or not person_ids:
            return []

        all_results: List[Dict] = []
        for i in range(0, len(person_ids), 50):
            batch = person_ids[i:i + 50]
            payload = {
                "only_verified_email": True,
                "enrich_mobile": False,
                "data": [
                    {"identifier": f"rec_{j}", "person_id": pid}
                    for j, pid in enumerate(batch)
                ]
            }
            print(f"[Prospeo] ⚡ bulk-enrich-person: {len(batch)} persons")
            result = await self._post("/bulk-enrich-person", payload)
            if result:
                items = (
                    result.get("response", {}).get("data")
                    or result.get("response", {}).get("results")
                    or []
                )
                all_results.extend(items)

        return all_results
