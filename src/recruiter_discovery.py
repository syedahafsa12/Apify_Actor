import asyncio
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx
from playwright.async_api import Browser, Page, TimeoutError as PlaywrightTimeout
from openai import AsyncOpenAI

import json

try:
    from mistralai import Mistral as _MistralClient
    _MISTRAL_AVAILABLE = True
except ImportError:
    _MISTRAL_AVAILABLE = False

try:
    import dns.resolver as _dns_resolver
    _DNS_AVAILABLE = True
except ImportError:
    _DNS_AVAILABLE = False

from .redis_cache import RedisCache, TTL_RECRUITER, TTL_GEMINI_CD
from .prospeo_client import ProspeoClient
from .enrichment_pipeline import EnrichmentPipeline

# =========================================================
# CONSTANTS
# =========================================================
JOB_BOARD_DOMAINS = [
    'gulftalent.com', 'indeed.com', 'jooble.org', 'adzuna.com',
    'linkedin.com', 'monster.com', 'glassdoor.com', 'naukri.com',
    'bayt.com', 'apply.workable.com', 'jobs.lever.co', 'greenhouse.io',
    'smartrecruiters.com', 'myworkdayjobs.com', 'icims.com',
]

_INVALID_DOMAIN_VALUES = {
    'not identified', 'notidentified', 'unknown', 'none', 'null', 'n/a', 'na',
    'localhost', 'not specified', 'notfound', 'not found', 'example.com',
    'test.com', 'company.com', 'domain.com', '',
}

_COMPANY_SUFFIX_RE = re.compile(
    r'\b(inc|llc|ltd|corp|co|company|group|global|solutions|technologies|tech|'
    r'services|innovation|innovations|consulting|consultants|partners|ventures|'
    r'systems|industries|enterprise|enterprises|agency|digital|labs|lab|studio|'
    r'studios|international|worldwide|network|networks|software|cloud|ai|data)\b',
    re.IGNORECASE,
)

_ALT_TLDS = ('com', 'io', 'co', 'net', 'org', 'ai', 'tech', 'app')


def _company_name_to_slug(company_name: str) -> str:
    """Derive a likely domain slug from a company name (strip suffixes, punctuation, spaces)."""
    name = company_name.lower().strip()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = _COMPANY_SUFFIX_RE.sub('', name).strip()
    name = re.sub(r'\s+', '', name)
    return name if len(name) >= 3 else ''


def _domain_candidates(original_domain: str, company_name: str = '') -> List[str]:
    """Return ordered list of alternative domains to probe when primary is unreachable."""
    parts = original_domain.rsplit('.', 1)
    base = parts[0] if len(parts) == 2 else original_domain
    original_tld = parts[1] if len(parts) == 2 else ''

    seen: Set[str] = {original_domain}
    candidates: List[str] = []

    for tld in _ALT_TLDS:
        cand = f"{base}.{tld}"
        if cand not in seen:
            seen.add(cand)
            candidates.append(cand)

    if company_name:
        slug = _company_name_to_slug(company_name)
        if slug and slug != base:
            for tld in _ALT_TLDS:
                cand = f"{slug}.{tld}"
                if cand not in seen:
                    seen.add(cand)
                    candidates.append(cand)

    return candidates


async def _find_live_alternative(original_domain: str, company_name: str = '') -> str:
    """Try alternative TLDs / name slugs concurrently; return first live domain or ''."""
    candidates = _domain_candidates(original_domain, company_name)
    if not candidates:
        return ''

    async def _probe(d: str) -> str:
        alive = await check_domain_alive(d)
        return d if alive else ''

    tasks = [asyncio.create_task(_probe(c)) for c in candidates]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            for t in tasks:
                t.cancel()
            return result
    return ''

# =========================================================
# DOMAIN HELPERS
# =========================================================
def normalize_domain(raw: str) -> str:
    """Return clean FQDN or '' if invalid/placeholder."""
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


def _is_ssl_error(error: Exception) -> bool:
    msg = str(error).lower()
    return any(kw in msg for kw in ['ssl', 'certificate', 'tls', 'cert'])


async def check_domain_alive(domain: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=8, follow_redirects=True, verify=False) as client:
            resp = await client.head(f"https://{domain}", headers={"User-Agent": "Mozilla/5.0"})
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
# AI COMPANY IDENTIFICATION
# =========================================================
class SmartCompanyDiscovery:
    def __init__(self, mistral_key: str = "", openai_key: str = "", cache: Optional[RedisCache] = None):
        self.cache = cache
        self._mistral_error_count = 0
        self.mistral_available = False

        if mistral_key and _MISTRAL_AVAILABLE:
            try:
                self._mistral = _MistralClient(api_key=mistral_key)
                self.mistral_available = True
                print("[AI] ✅ Mistral initialized (mistral-small-latest)")
            except Exception as e:
                print(f"[AI] ⚠️ Mistral init failed: {str(e)[:60]}")

        self.openai_available = False
        if openai_key:
            try:
                self.openai_client = AsyncOpenAI(api_key=openai_key)
                self.openai_available = True
                print("[AI] ✅ OpenAI initialized")
            except Exception as e:
                print(f"[AI] ⚠️ OpenAI init failed: {str(e)[:60]}")

    async def identify_company(
        self, job_description: str, job_title: str,
        company_hint: str = "", job_url: str = ""
    ) -> Optional[Dict]:
        prompt = f"""Identify the ACTUAL HIRING COMPANY from this job posting. Return JSON only.

Job Title: {job_title}
Company Hint: {company_hint}
URL: {job_url}
Description: {job_description[:3000]}

Rules:
- Ignore job boards and recruitment agencies
- Find the real employer's official domain
- Only mark confidence "high" if clearly identified
- List up to 3 plausible domains in order of likelihood (e.g. worthai.com, worth.ai, getworth.ai)

Return:
{{"company_name":"...","company_domain":"domain.com","domains":["domain.com","domain.io"],"company_location":"City, Country","industry":"...","is_recruitment_agency":false,"confidence":"high|medium|low","reasoning":"..."}}"""

        if self.mistral_available:
            result = await self._try_mistral(prompt)
            if result:
                return result

        if self.openai_available:
            return await self._try_openai(prompt)

        return None

    async def _try_mistral(self, prompt: str) -> Optional[Dict]:
        if self.cache and await self.cache.exists("mistral:cooldown"):
            print("[Mistral] ⏸️ In cooldown - skipping to OpenAI")
            return None
        try:
            response = await asyncio.wait_for(
                self._mistral.chat.complete_async(
                    model="mistral-small-latest",
                    messages=[{"role": "user", "content": prompt}]
                ),
                timeout=25,
            )
            text = (response.choices[0].message.content or "").strip()
            match = re.search(r'\{[\s\S]*?\}', text)
            if match:
                result = json.loads(match.group(0))
                self._mistral_error_count = 0
                print(f"[Mistral] ✅ {result.get('company_name')}")
                return result
            print("[Mistral] ⚠️ No JSON object in response")
        except asyncio.TimeoutError:
            print("[Mistral] ⏱️ Timeout")
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "rate" in err or "quota" in err:
                self._mistral_error_count += 1
                print(f"[Mistral] ⚠️ Rate limit #{self._mistral_error_count}")
                if self._mistral_error_count >= 3 and self.cache:
                    await self.cache.set("mistral:cooldown", "1", ttl=TTL_GEMINI_CD)
                    print("[Mistral] ❌ Entering 30min cooldown")
            else:
                print(f"[Mistral] ❌ {str(e)[:80]}")
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
                    temperature=0.5, max_tokens=512,
                    response_format={"type": "json_object"}
                ),
                timeout=25,
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
# HUNTER.IO FALLBACK
# =========================================================
class HunterEmailDiscovery:
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self.available = bool(api_key)
        if self.available:
            print("[Hunter.io] ✅ Initialized")
        else:
            print("[Hunter.io] ⚠️ No API key - disabled")

    async def find_company_emails(self, domain: str, company_name: str) -> List[str]:
        if not self.available or not domain:
            return []
        print(f"[Hunter.io] 🔍 Searching {domain}...")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://api.hunter.io/v2/domain-search",
                    params={'domain': domain, 'api_key': self.api_key, 'limit': 10, 'type': 'personal'}
                )
                if response.status_code == 200:
                    emails = []
                    for item in response.json().get('data', {}).get('emails', []):
                        email = item.get('value', '').lower().strip()
                        confidence = item.get('confidence', 0)
                        is_hr = any(kw in email for kw in ['hr', 'recruit', 'talent', 'career', 'hiring'])
                        if email and confidence >= (50 if is_hr else 70):
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
# PLAYWRIGHT SCRAPER FALLBACK
# =========================================================
class EnhancedEmailScraper:
    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email or '@' not in email:
            return False
        el = email.lower()
        if any(d in el for d in JOB_BOARD_DOMAINS):
            return False
        if any(p in el for p in ['noreply', 'no-reply', 'donotreply', 'webmaster', 'postmaster', 'abuse@', 'privacy@', 'legal@']):
            return False
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))

    @staticmethod
    async def _extract_emails_from_page(page: Page, domain: str) -> Set[str]:
        emails: Set[str] = set()
        try:
            content = await page.content()
            bare = domain.replace('www.', '')
            for email in re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content):
                el = email.lower()
                if (bare in el or any(kw in el for kw in ['hr', 'recruit', 'talent', 'career', 'hiring'])):
                    if EnhancedEmailScraper._is_valid_email(email):
                        emails.add(el)
        except Exception:
            pass
        return emails

    @staticmethod
    async def _find_contact_links(page: Page) -> List[str]:
        kws = ['contact', 'about', 'team', 'careers', 'jobs', 'hiring', 'recruit', 'join']
        found: List[str] = []
        for selector in ['nav a[href]', 'header a[href]', 'footer a[href]', '.navbar a[href]', '.menu a[href]']:
            try:
                for link in await page.query_selector_all(selector):
                    href = await link.get_attribute('href')
                    text = (await link.inner_text()).lower().strip()
                    if href and any(k in text for k in kws):
                        absolute = urljoin(page.url, href)
                        if urlparse(absolute).netloc == urlparse(page.url).netloc and absolute not in found:
                            found.append(absolute)
            except Exception:
                continue
        return found

    @staticmethod
    async def scrape_company_emails_smart(browser: Browser, company_domain: str, company_name: str) -> Set[str]:
        found: Set[str] = set()
        domain = normalize_domain(company_domain)
        if not domain:
            return found

        base_url = f"https://{domain}"
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
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
                    print(f"[Scraper] ⏱️ Timeout: {domain}")
                    break
                except Exception as e:
                    if _is_ssl_error(e) and not ignore_https:
                        ssl_failed = True
                        print(f"[Scraper] 🔄 SSL error - retrying with ignoreHTTPSErrors")
                        continue
                    print(f"[Scraper] ⚠️ {str(e)[:60]}")
                    break
                if resp and resp.status < 500:
                    found.update(await EnhancedEmailScraper._extract_emails_from_page(page, domain))
                    visited.add(base_url)
                    if len(found) < 3:
                        for link in (await EnhancedEmailScraper._find_contact_links(page))[:4]:
                            if link in visited or len(found) >= 5:
                                break
                            try:
                                r = await page.goto(link, timeout=12000, wait_until="domcontentloaded")
                                if r and r.status < 500:
                                    found.update(await EnhancedEmailScraper._extract_emails_from_page(page, domain))
                                visited.add(link)
                            except Exception:
                                continue
                break
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

        print(f"[Scraper] 📊 Found {len(found)} emails")
        return found


# =========================================================
# RECRUITER DISCOVERY — full priority chain
# =========================================================
class RecruiterDiscovery:
    """
    Priority: Prospeo (search→enrich) → Hunter → Scraping → AI generation
    """

    def __init__(
        self,
        browser: Browser,
        prospeo: ProspeoClient,
        cache: RedisCache,
        hunter_key: str = "",
        mistral_key: str = "",
        openai_key: str = "",
    ):
        self.browser = browser
        self.cache = cache
        self.pipeline = EnrichmentPipeline(prospeo, cache)
        self.hunter = HunterEmailDiscovery(hunter_key)
        self.scraper = EnhancedEmailScraper()
        self.company_ai = SmartCompanyDiscovery(mistral_key, openai_key, cache)

    async def discover(
        self,
        job_description: str,
        job_title: str,
        company_hint: str = "",
        job_url: str = "",
    ) -> Dict:
        print(f"[Discovery] 🚀 {company_hint or 'Unknown'}")

        # ── AI: identify real company + domain ───────────────────────────
        company_info = await self.company_ai.identify_company(
            job_description, job_title, company_hint, job_url
        )
        if not company_info:
            return _fail_result()

        domain = normalize_domain(company_info.get('company_domain', ''))

        # Build comprehensive candidate list:
        # 1. AI primary domain  2. AI domains array  3. TLD variants of primary base
        # Prospeo and Hunter work from domain strings — no reachability check needed for API calls.
        _seen_domains: set = set()
        _all_candidates: List[str] = []

        def _add_cand(d: str) -> None:
            d = normalize_domain(d)
            if d and d not in _seen_domains:
                _all_candidates.append(d)
                _seen_domains.add(d)

        _add_cand(domain)
        for _d in (company_info.get('domains') or []):
            _add_cand(_d)
        # TLD variants of the primary base (e.g. dignifysolutions.io, .co, .ai …)
        for _variant in _domain_candidates(domain, company_info.get('company_name', '')):
            _add_cand(_variant)

        if not _all_candidates:
            print(f"[Discovery] ⚠️ AI returned no valid domains — skipping {company_hint}")
            return _fail_result(company_info)

        company_name_display = company_info.get('company_name', company_hint or 'Unknown')
        print(f"[Discovery] 🏢 {company_name_display} | probing {len(_all_candidates)} domain candidates")

        emails: List[str] = []
        method = "none"
        pipeline_stats: Dict = {}
        winning_domain = _all_candidates[0]

        # ── STEP 1+2: Prospeo then Hunter for EVERY candidate (no reachability gate) ──
        for _cand in _all_candidates:
            _p_emails, _p_stats = await self.pipeline.discover_and_enrich(_cand)
            if _p_emails:
                emails = _p_emails
                pipeline_stats = _p_stats
                method = "prospeo"
                winning_domain = _cand
                print(f"[Discovery] ✅ Prospeo: {len(emails)} at {_cand} — credits used: {_p_stats.get('credits_used', 0)}")
                break

            _h_emails = await self.hunter.find_company_emails(_cand, company_name_display)
            if _h_emails:
                emails = _h_emails
                method = "hunter"
                winning_domain = _cand
                print(f"[Discovery] ✅ Hunter: {len(emails)} at {_cand}")
                break

            print(f"[Discovery] ℹ️ No results at {_cand} — trying next candidate")

        domain = winning_domain
        company_info['company_domain'] = winning_domain

        # ── STEP 3: Playwright scraping — only if primary domain is reachable ──
        if not emails:
            domain_alive = await check_domain_alive(_all_candidates[0])
            if domain_alive:
                scraped = await self.scraper.scrape_company_emails_smart(
                    self.browser, _all_candidates[0], company_name_display
                )
                emails = list(scraped)
                if emails:
                    method = "scraped"
                    domain = _all_candidates[0]
                    company_info['company_domain'] = domain
                else:
                    print(f"[Discovery] 🔄 Scraping empty — falling through to MX generation")
            else:
                print(f"[Discovery] ⏭️ Primary domain unreachable — skipping scraper")

        # ── STEP 4: MX-verified generation — try top 3 candidates ────────────
        if not emails:
            for _cand in _all_candidates[:3]:
                if await check_domain_has_mx(_cand):
                    emails = [f"careers@{_cand}", f"hr@{_cand}"]
                    method = "generated"
                    domain = _cand
                    company_info['company_domain'] = _cand
                    print(f"[Discovery] ✉️ Generated fallback emails (MX verified: {_cand})")
                    break
            if not emails:
                print(f"[Discovery] ❌ All {len(_all_candidates)} candidates exhausted")
                return _fail_result(company_info)

        emails = emails[:5]

        # ── Cache results for non-generated emails ────────────────────────
        if emails and method not in ("generated",):
            await self.cache.set_json(f"recruiter_email:{domain}", emails, ttl=TTL_RECRUITER)
            print(f"[Discovery] 🧠 Recruiter cached for {domain}")

        print(f"[Discovery] ✅ {len(emails)} emails via {method.upper()}")

        return {
            'success': bool(emails),
            'emails': emails,
            'company_info': company_info,
            'method': method,
            'cache_hit': pipeline_stats.get('cache_hits', 0) > 0 and method == "prospeo",
            'confidence': 'high' if method in ('prospeo', 'hunter', 'scraped') else 'low',
            'prospeo_stats': pipeline_stats,
        }


def _fail_result(company_info: Optional[Dict] = None) -> Dict:
    return {
        'success': False,
        'emails': [],
        'company_info': company_info,
        'method': 'failed',
        'cache_hit': False,
        'confidence': 'none',
        'prospeo_stats': {},
    }
