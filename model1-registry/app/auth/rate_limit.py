"""
Login rate limiting / lockout.

AuditReport1.md finding 2.2: `POST /api/v1/auth/login` had no throttling
of any kind - unlimited password guesses forever, from anyone. Low
urgency for the hackathon demo itself (the demo accounts are meant to be
publicly known), but a real gap the moment this is pointed at non-demo
accounts.

This is deliberately a simple in-memory, single-process limiter - no
Redis or DB table. That's the right amount of complexity for how this
app actually runs today (one `app` container in docker-compose, no
horizontal scaling anywhere in infra/). If that ever changes, this needs
a shared backend (e.g. Redis) instead, since separate processes would
each keep their own counters and the lockout would no longer be
effective across all of them.

Locking is keyed on (client IP, username) rather than username alone, so
one attacker can't use this to remotely lock a specific legitimate user
out of their own account from an arbitrary IP - they'd need to be
attacking from the same IP the real user logs in from.
"""

import time
import redis
from typing import Dict, List, Optional

class LoginRateLimiter:
    def __init__(self, max_attempts: int, window_seconds: float, lockout_seconds: float, redis_url: str):
        self.max_attempts = max_attempts
        self.window_seconds = int(window_seconds)
        self.lockout_seconds = int(lockout_seconds)
        self._redis = redis.from_url(redis_url, decode_responses=True)

    def _lock_key(self, key: str) -> str:
        return f"login:lock:{key}"

    def _fail_key(self, key: str) -> str:
        return f"login:fails:{key}"

    def seconds_until_unlocked(self, key: str) -> float:
        """0.0 if `key` may attempt a login right now, else how many
        seconds remain before it can."""
        ttl = self._redis.ttl(self._lock_key(key))
        if ttl > 0:
            return float(ttl)
        return 0.0

    def record_failure(self, key: str) -> None:
        fail_key = self._fail_key(key)
        fails = self._redis.incr(fail_key)
        if fails == 1:
            self._redis.expire(fail_key, self.window_seconds)
        
        if fails >= self.max_attempts:
            self._redis.setex(self._lock_key(key), self.lockout_seconds, "1")

    def record_success(self, key: str) -> None:
        """A successful login clears any accumulated failure count for
        this key - only *consecutive* failures should ever lock someone
        out, not a lifetime tally."""
        self._redis.delete(self._fail_key(key))
        self._redis.delete(self._lock_key(key))

    def reset_all(self) -> None:
        """Test-only convenience - production code never calls this."""
        for k in self._redis.scan_iter("login:*"):
            self._redis.delete(k)


def rate_limit_key(client_ip: str, username: str) -> str:
    return f"{client_ip}:{username.strip().lower()}"


def _build_default_limiter() -> LoginRateLimiter:
    from app.config import settings

    return LoginRateLimiter(
        max_attempts=settings.LOGIN_MAX_ATTEMPTS,
        window_seconds=settings.LOGIN_WINDOW_SECONDS,
        lockout_seconds=settings.LOGIN_LOCKOUT_SECONDS,
        redis_url=settings.REDIS_URL,
    )


login_rate_limiter = _build_default_limiter()

