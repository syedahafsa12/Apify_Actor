# =========================================================
# SCOPEAI ACTOR v7.0 - THIN ENQUEUE ACTOR
# =========================================================
# Architecture: discover jobs + recruiter emails → enqueue ONLY
# Backend worker handles ALL sending, SMTP rotation, retries
# =========================================================

import os
import re
import asyncio
import json
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Set
from urllib.parse import urlparse, urljoin

import httpx
from apify import Actor
from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeout
from dotenv import load_dotenv
load_dotenv()

import google.generativeai as genai
from openai import AsyncOpenAI

try:
    import dns.resolver as _dns_resolver
    _DNS_AVAILABLE = True
except ImportError:
    _DNS_AVAILABLE = False

try:
    from redis import asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

# =========================================================
# CONFIG
# =========================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
PROSPEO_API_KEY = os.getenv("PROSPEO_API_KEY", "")
REDIS_URL = os.getenv("REDIS_URL", "")

JOB_BOARD_DOMAINS = [
    'gulftalent.com', 'indeed.com', 'jooble.org', 'adzuna.com',
    'linkedin.com', 'monster.com', 'glassdoor.com', 'naukri.com',
    'bayt.com', 'apply.workable.com', 'jobs.lever.co', 'greenhouse.io',
    'smartrecruiters.com', 'myworkdayjobs.com', 'icims.com'
]

# =========================================================
# REDIS CACHE
# =========================================================
class RedisCache:
    def __init__(self, redis_url: str = ""):
        self._client = None
        self._fallback: Dict[str, str] = {}
        self._redis_url = redis_url
        self._available = False

    async def connect(self):
        if not _REDIS_AVAILABLE:
            print("[Redis] ⚠️ redis package not installed - using in-memory cache")
            return

        url = self._redis_url
        if not url:
            host = os.getenv("REDISHOST", "")
            port = os.getenv("REDISPORT", "6379")
            password = os.getenv("REDISPASSWORD", "")
            if host:
                url = f"redis://:{password}@{host}:{port}" if password else f"redis://{host}:{port}"

        if not url:
            print("[Redis] ⚠️ No Redis URL configured - using in-memory cache")
            return

        for attempt in range(1, 4):
            try:
                self._client = aioredis.from_url(
                    url, decode_responses=True, socket_connect_timeout=5
                )
                await self._client.ping()
                self._available = True
                print("[Redis] ✅ Connected")
                return
            except Exception as e:
                reason = str(e)[:80]
                if attempt < 3:
                    print(f"[Redis] ⚠️ Attempt {attempt}/3 failed: {reason} - retrying...")
                    await asyncio.sleep(1)
                else:
                    print(f"[Redis] ⚠️ All 3 attempts failed: {reason} - using in-memory cache")
                    self._client = None

    async def get(self, key: str) -> Optional[str]:
        try:
            if self._available and self._client:
                return await self._client.get(key)
        except Exception:
            pass
        return self._fallback.get(key)

    async def set(self, key: str, value: str, ttl: int = 86400):
        try:
            if self._available and self._client:
                await self._client.setex(key, ttl, value)
                return
        except Exception:
            pass
        self._fallback[key] = value

    async def exists(self, key: str) -> bool:
        try:
            if self._available and self._client:
                return bool(await self._client.exists(key))
        except Exception:
            pass
        return key in self._fallback

    async def close(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass

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
            "job_id": self.job_id,
            "job_title": self.job_title,
            "company_name": self.company_name,
            "status": self.status,
            "error_message": self.error_message,
            "application_url": self.application_url,
            "applied_at": self.applied_at
        }

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

def is_valid_api_base(api_base: str) -> bool:
    return bool(normalize_api_base(api_base))

# =========================================================
# DOMAIN VALIDATION HELPERS
# =========================================================
def _is_ssl_error(error: Exception) -> bool:
    msg = str(error).lower()
    return any(kw in msg for kw in ['ssl', 'certificate', 'tls', 'cert'])

_INVALID_DOMAIN_VALUES = {
    'not identified', 'notidentified', 'unknown', 'none', 'null', 'n/a', 'na',
    'localhost', 'not specified', 'notfound', 'not found', 'example.com',
    'test.com', 'company.com', 'domain.com', ''
}

def normalize_domain(raw: str) -> str:
    """Normalize to clean FQDN. Returns '' if invalid/placeholder."""
    if not raw:
        return ""
    domain = raw.lower().strip()
    domain = re.sub(r'^https?://', '', domain)
    domain = domain.split('/')[0].split('?')[0].split('#')[0].strip()
    domain = domain.replace(' ', '')
    if not domain or domain in _INVALID_DOMAIN_VALUES:
        return ""
    if '.' not in domain:
        return ""
    tld = domain.rsplit('.', 1)[-1]
    if not re.match(r'^[a-z]{2,6}$', tld):
        return ""
    if not re.match(r'^[a-z0-9]([a-z0-9.\-]*[a-z0-9])?$', domain):
        return ""
    return domain

async def check_domain_alive(domain: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True, verify=False) as client:
            resp = await client.head(
                f"https://{domain}",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            return resp.status_code < 500
    except Exception:
        return False

async def check_domain_has_mx(domain: str) -> bool:
    if not _DNS_AVAILABLE:
        return True
    try:
        records = await asyncio.wait_for(
            asyncio.to_thread(_dns_resolver.resolve, domain, 'MX'),
            timeout=5
        )
        return len(records) > 0
    except Exception:
        return False

# =========================================================
# SMART COMPANY DISCOVERY (with Gemini cooldown)
# =========================================================
class SmartCompanyDiscovery:
    def __init__(self, gemini_key: str = None, openai_key: str = None, cache: RedisCache = None):
        self.cache = cache
        self._gemini_error_count = 0
        self.gemini_available = False

        if gemini_key:
            try:
                genai.configure(api_key=gemini_key)
                self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
                self.gemini_available = True
                print("[AI] ✅ Gemini initialized")
            except Exception as e:
                print(f"[AI] ⚠️ Gemini init failed: {str(e)[:60]}")

        self.openai_available = False
        if openai_key:
            try:
                self.openai_client = AsyncOpenAI(api_key=openai_key)
                self.openai_available = True
                print("[AI] ✅ OpenAI initialized")
            except Exception as e:
                print(f"[AI] ⚠️ OpenAI init failed: {str(e)[:60]}")

    async def identify_company(
        self,
        job_description: str,
        job_title: str,
        company_name_hint: str = "",
        job_url: str = ""
    ) -> Optional[Dict]:
        prompt = f"""Identify the ACTUAL HIRING COMPANY from this job posting. Return JSON only.

Job Title: {job_title}
Company Hint: {company_name_hint}
URL: {job_url}
Description: {job_description[:3000]}

Rules:
- Ignore job boards and recruitment agencies
- Find the real employer's official domain
- Only mark confidence "high" if clearly identified

Return this exact JSON:
{{"company_name":"...","company_domain":"domain.com","company_location":"City, Country","industry":"...","is_recruitment_agency":false,"confidence":"high|medium|low","reasoning":"..."}}"""

        if self.gemini_available:
            result = await self._try_gemini(prompt)
            if result:
                return result

        if self.openai_available:
            return await self._try_openai(prompt)

        return None

    async def _try_gemini(self, prompt: str) -> Optional[Dict]:
        if self.cache and await self.cache.exists("gemini:cooldown"):
            print("[Gemini] ⏸️ In cooldown - skipping to OpenAI")
            return None

        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.gemini_model.generate_content,
                    prompt,
                    generation_config={'temperature': 0.5, 'max_output_tokens': 512}
                ),
                timeout=25
            )
            json_match = re.search(r'\{[\s\S]*?\}', response.text.strip())
            if json_match:
                result = json.loads(json_match.group(0))
                self._gemini_error_count = 0
                print(f"[Gemini] ✅ {result.get('company_name')}")
                return result
        except asyncio.TimeoutError:
            print("[Gemini] ⏱️ Timeout")
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "quota" in err or "rate" in err:
                self._gemini_error_count += 1
                print(f"[Gemini] ⚠️ Quota error #{self._gemini_error_count}")
                if self._gemini_error_count >= 3 and self.cache:
                    await self.cache.set("gemini:cooldown", "1", ttl=1800)
                    print("[Gemini] ❌ Entering 30min cooldown - switching to OpenAI")
            else:
                print(f"[Gemini] ❌ {str(e)[:80]}")
        return None

    async def _try_openai(self, prompt: str) -> Optional[Dict]:
        try:
            response = await asyncio.wait_for(
                self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Return valid JSON only."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.5,
                    max_tokens=512,
                    response_format={"type": "json_object"}
                ),
                timeout=25
            )
            result = json.loads(response.choices[0].message.content.strip())
            print(f"[OpenAI] ✅ {result.get('company_name')}")
            return result
        except asyncio.TimeoutError:
            print("[OpenAI] ⏱️ Timeout")
        except Exception as e:
            print(f"[OpenAI] ❌ {str(e)[:80]}")
        return None

# =========================================================
# ENHANCED EMAIL SCRAPER (parallel-safe, SSL retry)
# =========================================================
class EnhancedEmailScraper:
    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email or '@' not in email:
            return False
        email_lower = email.lower()
        for domain in JOB_BOARD_DOMAINS:
            if domain in email_lower:
                return False
        excluded = ['noreply', 'no-reply', 'donotreply', 'webmaster', 'postmaster', 'abuse@', 'privacy@', 'legal@']
        if any(p in email_lower for p in excluded):
            return False
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

    @staticmethod
    async def _extract_emails_from_page(page: Page, domain: str) -> Set[str]:
        emails: Set[str] = set()
        try:
            content = await page.content()
            for email in re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content):
                email_lower = email.lower()
                is_company = domain.replace('www.', '') in email_lower
                is_hiring = any(kw in email_lower for kw in ['career', 'hr', 'recruit', 'talent', 'jobs', 'hiring'])
                if (is_company or is_hiring) and EnhancedEmailScraper._is_valid_email(email):
                    emails.add(email_lower)
        except Exception:
            pass
        return emails

    @staticmethod
    async def _find_contact_links(page: Page) -> List[str]:
        keywords = ['contact', 'about', 'team', 'careers', 'jobs', 'hiring', 'recruit', 'join']
        found: List[str] = []
        selectors = ['nav a[href]', 'header a[href]', 'footer a[href]', '.navbar a[href]', '.menu a[href]']
        for selector in selectors:
            try:
                for link in await page.query_selector_all(selector):
                    href = await link.get_attribute('href')
                    text = (await link.inner_text()).lower().strip()
                    if href and any(kw in text for kw in keywords):
                        absolute = urljoin(page.url, href)
                        parsed_abs = urlparse(absolute)
                        parsed_page = urlparse(page.url)
                        if parsed_abs.netloc == parsed_page.netloc and absolute not in found:
                            found.append(absolute)
            except Exception:
                continue
        return found

    @staticmethod
    async def scrape_company_emails_smart(
        browser: Browser,
        company_domain: str,
        company_name: str
    ) -> Set[str]:
        found_emails: Set[str] = set()
        if not company_domain:
            return found_emails

        domain = company_domain.replace('http://', '').replace('https://', '').split('/')[0].strip()
        if not domain:
            return found_emails

        base_url = f"https://{domain}"
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        print(f"[Scraper] 🔍 Scraping: {domain}")

        ssl_failed = False

        for ignore_https in [False, True]:
            if ignore_https and not ssl_failed:
                break

            ctx = None
            try:
                ctx = await browser.new_context(user_agent=ua, ignore_https_errors=ignore_https)
                page = await ctx.new_page()
                visited: Set[str] = set()

                try:
                    resp = await page.goto(base_url, timeout=20000, wait_until="domcontentloaded")
                except PlaywrightTimeout:
                    print(f"[Scraper] ⏱️ Timeout loading {domain}")
                    break
                except Exception as e:
                    if _is_ssl_error(e) and not ignore_https:
                        ssl_failed = True
                        print(f"[Scraper] 🔄 SSL error - retrying with ignoreHTTPSErrors")
                        continue
                    print(f"[Scraper] ⚠️ Load error: {str(e)[:60]}")
                    break

                if resp and resp.status < 500:
                    homepage_emails = await EnhancedEmailScraper._extract_emails_from_page(page, domain)
                    found_emails.update(homepage_emails)
                    visited.add(base_url)

                    if len(found_emails) < 3:
                        contact_links = await EnhancedEmailScraper._find_contact_links(page)
                        for link in contact_links[:4]:
                            if link in visited or len(found_emails) >= 5:
                                break
                            try:
                                r = await page.goto(link, timeout=12000, wait_until="domcontentloaded")
                                if r and r.status < 500:
                                    found_emails.update(
                                        await EnhancedEmailScraper._extract_emails_from_page(page, domain)
                                    )
                                visited.add(link)
                            except Exception:
                                continue

                break  # Success

            except Exception as e:
                if _is_ssl_error(e) and not ignore_https:
                    ssl_failed = True
                    print(f"[Scraper] 🔄 SSL error - retrying with ignoreHTTPSErrors")
                else:
                    print(f"[Scraper] ⚠️ {str(e)[:60]}")
                    break
            finally:
                if ctx:
                    try:
                        await ctx.close()
                    except Exception:
                        pass

        print(f"[Scraper] 📊 Found {len(found_emails)} emails")
        return found_emails

# =========================================================
# PROSPEO EMAIL DISCOVERY (PRIMARY provider)
# =========================================================
class ProspeoEmailDiscovery:
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.available = bool(api_key)
        self._lock = asyncio.Lock()
        self._last_call = 0.0
        self._window_start = time.monotonic()
        self._window_count = 0

        if self.available:
            print("[Prospeo] ✅ Initialized")
        else:
            print("[Prospeo] ⚠️ No API key - disabled")

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

    async def find_company_emails(self, domain: str, company_name: str) -> List[str]:
        if not self.available:
            return []

        clean = normalize_domain(domain)
        if not clean:
            print(f"[Prospeo] ⏭️ Invalid domain '{domain}' - skipping")
            return []

        print(f"[Prospeo] 🔍 Searching {clean}...")
        await self._rate_limit()

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    "https://api.prospeo.io/domain-search",
                    json={"company": clean, "limit": 10},
                    headers={"X-KEY": self.api_key, "Content-Type": "application/json"}
                )

                if response.status_code == 200:
                    data = response.json()
                    if data.get("error"):
                        print(f"[Prospeo] ❌ {data.get('message', 'API error')}")
                        return []

                    emails = []
                    for item in data.get("response", {}).get("emails", []):
                        email = item.get("email", "").lower().strip()
                        validity = item.get("validity", "unknown")
                        if validity == "valid" and email:
                            emails.append(email)

                    print(f"[Prospeo] ✅ {len(emails)} valid emails")
                    return emails[:5]

                if response.status_code == 400:
                    print(f"[Prospeo] ⚠️ Bad request for domain: {clean}")
                elif response.status_code == 429:
                    print("[Prospeo] ⚠️ Rate limit exceeded")
                elif response.status_code == 401:
                    print("[Prospeo] ❌ Invalid API key")
                    self.available = False
                else:
                    print(f"[Prospeo] ❌ HTTP {response.status_code}")

        except asyncio.TimeoutError:
            print("[Prospeo] ⏱️ Timeout")
        except Exception as e:
            print(f"[Prospeo] ❌ {str(e)[:80]}")

        return []

# =========================================================
# HUNTER.IO EMAIL DISCOVERY (FALLBACK only)
# =========================================================
class HunterEmailDiscovery:
    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.available = bool(api_key)

        if self.available:
            print("[Hunter.io] ✅ Initialized")
        else:
            print("[Hunter.io] ⚠️ No API key - disabled")

    async def find_company_emails(self, domain: str, company_name: str) -> List[str]:
        if not self.available or not domain:
            return []

        domain = domain.replace('http://', '').replace('https://', '').split('/')[0].strip()
        print(f"[Hunter.io] 🔍 Searching {domain}...")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={
                        'domain': domain,
                        'api_key': self.api_key,
                        'limit': 10,
                        'type': 'personal'
                    }
                )

                if response.status_code == 200:
                    emails = []
                    for item in response.json().get('data', {}).get('emails', []):
                        email = item.get('value', '').lower().strip()
                        confidence = item.get('confidence', 0)
                        is_hr = any(kw in email for kw in ['hr', 'recruit', 'talent', 'career', 'hiring', 'jobs'])
                        min_conf = 50 if is_hr else 70
                        if email and confidence >= min_conf:
                            emails.append(email)
                            print(f"[Hunter.io]   ✅ {email} ({confidence}%)")
                    print(f"[Hunter.io] ✅ {len(emails)} emails")
                    return emails[:5]

                if response.status_code == 429:
                    print("[Hunter.io] ⚠️ Rate limit exceeded")
                elif response.status_code == 401:
                    print("[Hunter.io] ❌ Invalid API key")
                else:
                    print(f"[Hunter.io] ❌ HTTP {response.status_code}")

        except asyncio.TimeoutError:
            print("[Hunter.io] ⏱️ Timeout")
        except Exception as e:
            print(f"[Hunter.io] ❌ {str(e)[:80]}")

        return []

# =========================================================
# COMPLETE EMAIL DISCOVERY
# Priority: Redis cache → Prospeo → Hunter → Scraping → AI gen
# =========================================================
class CompleteEmailDiscovery:
    def __init__(
        self,
        browser: Browser,
        gemini_key: str = None,
        openai_key: str = None,
        hunter_key: str = None,
        prospeo_key: str = None,
        cache: RedisCache = None
    ):
        self.browser = browser
        self.cache = cache or RedisCache()
        self.company_discovery = SmartCompanyDiscovery(gemini_key, openai_key, self.cache)
        self.scraper = EnhancedEmailScraper()
        self.prospeo = ProspeoEmailDiscovery(prospeo_key)
        self.hunter = HunterEmailDiscovery(hunter_key)

    async def discover_emails_for_job(
        self,
        job_description: str,
        job_title: str,
        company_name_hint: str = "",
        job_url: str = ""
    ) -> Dict:
        print(f"[Discovery] 🚀 {company_name_hint or 'Unknown'}")

        company_info = await self.company_discovery.identify_company(
            job_description, job_title, company_name_hint, job_url
        )
        if not company_info:
            return {
                'success': False, 'emails': [], 'company_info': None,
                'method': 'failed', 'cache_hit': False, 'confidence': 'none'
            }

        raw_domain = company_info.get('company_domain', '')
        domain = normalize_domain(raw_domain)

        if not domain:
            print(f"[Discovery] ⚠️ AI returned invalid domain '{raw_domain}' for {company_name_hint}")
            return {
                'success': False, 'emails': [], 'company_info': company_info,
                'method': 'failed', 'cache_hit': False, 'confidence': 'none'
            }

        print(f"[Discovery] 🏢 {company_info['company_name']} | 🌐 {domain}")

        # Step 1: Redis cache
        cache_key = f"recruiter_email:{domain}"
        cached = await self.cache.get(cache_key)
        if cached:
            try:
                emails = json.loads(cached)
                if emails:
                    print(f"[Discovery] 🔁 Using cached recruiter for {domain}")
                    return {
                        'success': True, 'emails': emails, 'company_info': company_info,
                        'method': 'cache', 'cache_hit': True, 'confidence': 'high'
                    }
            except Exception:
                pass

        emails: List[str] = []
        method = 'none'

        # Step 2: Prospeo (PRIMARY)
        if not emails:
            emails = await self.prospeo.find_company_emails(domain, company_info['company_name'])
            if emails:
                method = 'prospeo'

        # Step 3: Hunter (fallback)
        if not emails:
            print(f"[Discovery] 🔄 Prospeo empty - trying Hunter.io...")
            emails = await self.hunter.find_company_emails(domain, company_info['company_name'])
            if emails:
                method = 'hunter'

        # Step 4: Scraping (fallback)
        if not emails:
            print(f"[Discovery] 🔄 APIs empty - trying scraping...")
            scraped = await self.scraper.scrape_company_emails_smart(
                self.browser, domain, company_info['company_name']
            )
            emails = list(scraped)
            if emails:
                method = 'scraped'

        # Step 5: AI generation - last resort, domain verified only, max 2 emails
        if not emails and domain:
            print(f"[Discovery] ⚠️ All sources empty - verifying domain before generation...")
            alive = await check_domain_alive(domain)
            has_mx = await check_domain_has_mx(domain)
            if alive and has_mx:
                emails = [f"careers@{domain}", f"hr@{domain}"]
                method = 'generated'
                print(f"[Discovery] ✉️ Generated 2 fallback emails (domain verified + MX exists)")
            else:
                print(f"[Discovery] ❌ Domain {domain} not verified or no MX - skipping generation")

        emails = emails[:5]

        # Cache discovered emails (not generated ones - too unreliable)
        if emails and method != 'generated':
            await self.cache.set(cache_key, json.dumps(emails), ttl=86400)
            print(f"[Discovery] 🧠 Recruiter cached for {domain}")

        print(f"[Discovery] ✅ {len(emails)} emails via {method.upper()}")

        return {
            'success': bool(emails),
            'emails': emails,
            'company_info': company_info,
            'method': method,
            'cache_hit': False,
            'confidence': 'high' if method in ('cache', 'prospeo', 'hunter', 'scraped') else 'low'
        }

# =========================================================
# ENQUEUE (one POST per email to /v1/automation/email-apply)
# =========================================================
def _build_cover_letter(cv_json: Dict, job_title: str, company: str) -> str:
    name = cv_json.get('name', 'Job Applicant')
    summary = cv_json.get('summary', '')
    skills = cv_json.get('skills', [])
    email = cv_json.get('email') or cv_json.get('contact', {}).get('email', '')
    body = f"Dear {company} Hiring Team,\n\nI am writing to express my interest in the {job_title} position at {company}.\n\n"
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
    email_source: str = "unknown"
) -> bool:
    """Enqueue single email to backend queue. Returns True if successfully queued."""
    normalized = normalize_api_base(api_base)
    if not normalized:
        return False

    job_id = str(job.get("id") or job.get("job_id") or "")
    job_title = job.get("title", "")
    company = job.get("company", "")
    job_url = job.get("url") or job.get("link", "")

    # Dedup: skip if already enqueued this run+job+email combination
    dedup_key = f"sent:{run_id}:{job_id}:{to_email}"
    if await cache.exists(dedup_key):
        print(f"[Enqueue] ⏭️ Duplicate: {to_email}")
        return False

    # Recruiter memory: skip if already contacted this specific recruiter (cross-run)
    recruiter_key = f"recruiter:{to_email}"
    if await cache.exists(recruiter_key):
        print(f"[Enqueue] ⏭️ Recruiter already contacted: {to_email}")
        return False

    applicant_name = cv_json.get('name', '')
    applicant_email = cv_json.get('email') or cv_json.get('contact', {}).get('email', '')
    applicant_phone = cv_json.get('phone') or cv_json.get('contact', {}).get('phone', '') or ''
    cover_letter = _build_cover_letter(cv_json, job_title, company)

    payload = {
        "run_id": run_id,
        "job_url": job_url,
        "job_title": job_title,
        "company": company,
        "to_email": to_email,
        "cv_file_url": cv_file_url,
        "cover_letter": cover_letter,
        "applicant_name": applicant_name,
        "applicant_email": applicant_email,
        "applicant_phone": applicant_phone,
        "ai_discovery": {"email_source": email_source, "user_id": user_id}
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{normalized}/v1/automation/email-apply",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code in (200, 201, 202):
                ttl_7d = 7 * 24 * 3600
                await cache.set(dedup_key, "1", ttl=ttl_7d)
                await cache.set(recruiter_key, "1", ttl=ttl_7d)
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
    email_source: str = "unknown"
) -> int:
    """Enqueue all emails for a job. Returns count of successfully queued emails."""
    company = job.get("company", "")
    company_key = f"recruiter_company:{company.lower().replace(' ', '_')[:60]}"

    # Company-level cross-run dedup: if this company was already contacted, skip all
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

    # Mark company as contacted after all emails for this job are processed
    if queued > 0:
        await cache.set(company_key, "1", ttl=7 * 24 * 3600)

    return queued

# =========================================================
# SSE LOGGING
# =========================================================
async def log_to_backend(run_id: str, user_id: str, api_base: str, message: dict):
    try:
        normalized = normalize_api_base(api_base)
        if not normalized:
            return
        message["run_id"] = run_id
        message["user_id"] = user_id
        message["ts"] = datetime.now(timezone.utc).isoformat()
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{normalized}/v1/automation/sse-log", json=message)
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
    error_message: str = None
) -> bool:
    normalized = normalize_api_base(api_base)
    if not normalized:
        print("[Callback] ⚠️ No valid API_BASE for callback")
        return False

    successful = len([jr for jr in job_results if jr.status == "success"])
    failed = len([jr for jr in job_results if jr.status in ("failed", "skipped")])

    payload = {
        "run_id": run_id,
        "user_id": user_id,
        "status": status,
        "total_jobs": total_jobs,
        "successful_applications": successful,
        "failed_applications": failed,
        "job_results": [jr.to_dict() for jr in job_results],
        "error_message": error_message,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{normalized}/v1/automation/callback",
                json=payload,
                headers={"Content-Type": "application/json"}
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
    failure_reason: str = None
):
    total_jobs = len(job_results)

    try:
        duration = max(1, int((datetime.utcnow() - started).total_seconds()))

        if status == "failed":
            await log_to_backend(run_id, user_id, api_base, {
                "type": "error",
                "msg": f"❌ Automation failed: {failure_reason or 'Unknown error'}",
                "duration": duration
            })
        else:
            await log_to_backend(run_id, user_id, api_base, {
                "type": "completion",
                "msg": "🎉 Automation completed! ⚡ Worker will process asynchronously",
                "run_id": run_id,
                "user_id": user_id,
                "jobs_processed": stats.get("processed", 0),
                "recruiters_discovered": stats.get("recruiters_discovered", 0),
                "emails_enqueued": stats.get("emails_enqueued", 0),
                "cache_hits": stats.get("cache_hits", 0),
                "prospeo_hits": stats.get("prospeo_hits", 0),
                "hunter_fallbacks": stats.get("hunter_fallbacks", 0),
                "ai_fallbacks": stats.get("ai_fallbacks", 0),
                "duration": duration,
                "ts": datetime.now(timezone.utc).isoformat()
            })

        await send_callback_to_backend(
            run_id, user_id, api_base, status, total_jobs, job_results, failure_reason
        )

    except Exception as e:
        print(f"[Finish] ⚠️ {e}")

# =========================================================
# MAIN ACTOR
# =========================================================
async def run_actor():
    async with Actor() as actor:
        input_data = await actor.get_input() or {}

        api_base_raw = input_data.get("API_BASE") or os.getenv("API_BASE", "")
        api_base = normalize_api_base(api_base_raw)

        gemini_key = input_data.get("GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY", "")
        openai_key = input_data.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY", "")
        hunter_key = input_data.get("HUNTER_API_KEY") or os.getenv("HUNTER_API_KEY", "")
        prospeo_key = input_data.get("PROSPEO_API_KEY") or os.getenv("PROSPEO_API_KEY", "")
        redis_url = input_data.get("REDIS_URL") or os.getenv("REDIS_URL", "")

        run_id = input_data.get("run_id")
        user_id = input_data.get("user_id")
        jobs = input_data.get("jobs", [])
        cv_json = input_data.get("cv_json", {})
        cv_file_url = input_data.get("cv_file_url", "")
        max_jobs = int(input_data.get("max_jobs", 10))

        started = datetime.utcnow()
        job_results: List[JobResult] = []
        failure_reason = None
        stats_lock = asyncio.Lock()

        stats = {
            "total_jobs": len(jobs),
            "processed": 0,
            "recruiters_discovered": 0,
            "emails_enqueued": 0,
            "cache_hits": 0,
            "prospeo_hits": 0,
            "hunter_fallbacks": 0,
            "scraping_hits": 0,
            "ai_fallbacks": 0,
            "applications_success": 0,
            "applications_failed": 0
        }

        actor.log.info("=" * 60)
        actor.log.info("🚀 SCOPEAI ACTOR v7.0 - THIN ENQUEUE ACTOR")
        actor.log.info("=" * 60)
        actor.log.info(f"Run ID: {run_id} | User ID: {user_id} | Jobs: {len(jobs)}")
        actor.log.info(f"API Base: {api_base}")
        actor.log.info("=" * 60)

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
                "total_jobs": len(jobs)
            })

            jobs = jobs[:max_jobs]

            for idx, job in enumerate(jobs):
                job_id = job.get("id") or job.get("job_id") or f"job_{idx}"
                job["id"] = job_id
                job_result = JobResult(
                    str(job_id),
                    job.get("title", "Unknown Title"),
                    job.get("company", "Unknown Company")
                )
                job_results.append(job_result)
                actor.log.info(f"[Init] Job {idx}: ID={job_id} | {job.get('title')} @ {job.get('company')}")

            cache = RedisCache(redis_url)
            await cache.connect()

            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
                )

                discovery = CompleteEmailDiscovery(
                    browser, gemini_key, openai_key, hunter_key, prospeo_key, cache
                )

                sem = asyncio.Semaphore(3)

                async def process_job(job_idx: int, job: Dict, job_result: JobResult):
                    async with sem:
                        title = job.get("title", "")
                        url = job.get("url") or job.get("link", "")
                        company = job.get("company", "")
                        description = job.get("description") or job.get("snippet", "")

                        actor.log.info(f"[{job_idx}/{len(jobs)}] 📋 {title} @ {company}")

                        if not title or not company or not description:
                            job_result.mark_skipped("Missing title, company, or description")
                            async with stats_lock:
                                stats["applications_failed"] += 1
                            return

                        try:
                            await log_to_backend(run_id, user_id, api_base, {
                                "type": "info",
                                "msg": f"🔍 Processing {job_idx}/{len(jobs)}: {title} at {company}",
                                "job_title": title,
                                "company": company,
                                "progress": f"{job_idx}/{len(jobs)}"
                            })

                            email_result = await discovery.discover_emails_for_job(
                                description, title, company, url
                            )

                            if not email_result['success'] or not email_result['emails']:
                                actor.log.warning(f"⚠️ No emails found: {company}")
                                job_result.mark_failed("No valid emails discovered")
                                async with stats_lock:
                                    stats["applications_failed"] += 1
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type": "warning",
                                    "msg": f"⚠️ No emails found for {company}",
                                    "job_title": title,
                                    "company": company
                                })
                                return

                            emails = email_result['emails']
                            company_info = email_result['company_info']
                            method = email_result['method']
                            cache_hit = email_result.get('cache_hit', False)

                            job_result.company_name = company_info['company_name']

                            async with stats_lock:
                                stats["recruiters_discovered"] += 1
                                if cache_hit:
                                    stats["cache_hits"] += 1
                                elif method == 'prospeo':
                                    stats["prospeo_hits"] += 1
                                elif method == 'hunter':
                                    stats["hunter_fallbacks"] += 1
                                elif method == 'scraped':
                                    stats["scraping_hits"] += 1
                                elif method == 'generated':
                                    stats["ai_fallbacks"] += 1

                            if cache_hit:
                                actor.log.info(f"🔁 Using cached recruiter: {company_info['company_name']}")
                            else:
                                actor.log.info(f"✅ {len(emails)} emails via {method.upper()}: {company_info['company_name']}")

                            await log_to_backend(run_id, user_id, api_base, {
                                "type": "info",
                                "msg": (
                                    f"🔁 Using cached recruiter for {company_info['company_name']}"
                                    if cache_hit else
                                    f"✅ Found {len(emails)} emails via {method} for {company_info['company_name']}"
                                ),
                                "job_title": title,
                                "company": company_info['company_name'],
                                "emails_found": len(emails),
                                "method": method
                            })

                            enqueued = await enqueue_emails_for_job(
                                run_id, user_id, job, emails, cv_json, cv_file_url,
                                api_base, cache, email_source=method
                            )

                            if enqueued > 0:
                                job_result.mark_success(
                                    emails_enqueued=enqueued,
                                    application_url=url
                                )
                                async with stats_lock:
                                    stats["emails_enqueued"] += enqueued
                                    stats["applications_success"] += 1
                                    stats["processed"] += 1

                                actor.log.info(
                                    f"📥 Enqueued {enqueued}/{len(emails)} emails for {company_info['company_name']} "
                                    f"⚡ Worker will process asynchronously"
                                )
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type": "success",
                                    "msg": (
                                        f"📥 Enqueued {enqueued}/{len(emails)} emails for {company_info['company_name']} "
                                        f"⚡ Worker will process asynchronously"
                                    ),
                                    "job_title": title,
                                    "company": company_info['company_name'],
                                    "emails_enqueued": enqueued
                                })
                            else:
                                job_result.mark_failed("Enqueue request failed")
                                async with stats_lock:
                                    stats["applications_failed"] += 1
                                    stats["processed"] += 1
                                await log_to_backend(run_id, user_id, api_base, {
                                    "type": "warning",
                                    "msg": f"❌ Enqueue failed for {company_info['company_name']}",
                                    "job_title": title,
                                    "company": company_info['company_name']
                                })

                        except Exception as e:
                            actor.log.error(f"❌ Job error [{title}]: {str(e)[:100]}")
                            import traceback
                            actor.log.error(traceback.format_exc())
                            job_result.mark_failed(f"Processing error: {str(e)[:100]}")
                            async with stats_lock:
                                stats["applications_failed"] += 1
                            await log_to_backend(run_id, user_id, api_base, {
                                "type": "error",
                                "msg": f"❌ Error processing {title}: {str(e)[:100]}",
                                "job_title": title,
                                "company": company
                            })

                tasks = [
                    process_job(idx + 1, job, jr)
                    for idx, (job, jr) in enumerate(zip(jobs, job_results))
                ]
                await asyncio.gather(*tasks)

                await browser.close()

            await cache.close()

            successful = len([jr for jr in job_results if jr.status == "success"])
            failed = len([jr for jr in job_results if jr.status in ("failed", "skipped")])

            if successful > 0 and failed > 0:
                final_status = "partial"
            elif successful > 0:
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
            actor.log.info(f"✅ Applications Success:  {stats['applications_success']}")
            actor.log.info(f"❌ Applications Failed:   {stats['applications_failed']}")
            duration = int((datetime.utcnow() - started).total_seconds())
            actor.log.info(f"⏱️  Duration:              {duration}s")
            actor.log.info("=" * 60)

        except Exception as e:
            failure_reason = str(e)
            actor.log.error(f"💥 FATAL ERROR: {e}")
            import traceback
            actor.log.error(traceback.format_exc())
            await finish_automation_run(
                run_id, job_results, "failed", started, api_base, stats, user_id, failure_reason
            )

def main():
    asyncio.run(run_actor())

if __name__ == "__main__":
    main()
