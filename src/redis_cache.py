import os
import asyncio
import json
from typing import Any, Dict, Optional

try:
    from redis import asyncio as aioredis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

# TTL constants (seconds)
TTL_DOMAIN_SCAN    = 30 * 24 * 3600   # 30 days  - company:scanned:{domain}
TTL_PERSON_ENRICH  = 90 * 24 * 3600   # 90 days  - person:enrich:{person_id}
TTL_RECRUITER      = 24 * 3600         # 24 hours - recruiter_email:{domain}
TTL_7DAYS          = 7  * 24 * 3600   # 7 days   - sent/recruiter/company keys
TTL_GEMINI_CD      = 1800              # 30 min   - gemini:cooldown


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

        url = (
            self._redis_url
            or os.getenv("REDIS_PUBLIC_URL", "")
            or os.getenv("REDIS_URL", "")
        )
        if not url:
            host     = os.getenv("REDISHOST", "")
            port     = os.getenv("REDISPORT", "6379")
            password = os.getenv("REDIS_PASSWORD", "") or os.getenv("REDISPASSWORD", "")
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

    async def set(self, key: str, value: str, ttl: int = TTL_RECRUITER):
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

    async def get_json(self, key: str) -> Optional[Any]:
        raw = await self.get(key)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass
        return None

    async def set_json(self, key: str, value: Any, ttl: int = TTL_RECRUITER):
        await self.set(key, json.dumps(value), ttl=ttl)

    async def close(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
