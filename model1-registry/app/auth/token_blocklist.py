"""
Token blocklist for JWT revocation on logout.

BUG-010 fix: Logout now adds the token's jti to this blocklist so that
captured JWTs cannot be replayed after a user logs out.
"""

import redis
from app.config import settings

# Shared redis connection for blocklist
_redis = redis.from_url(settings.REDIS_URL, decode_responses=True)

def _blocklist_key(jti: str) -> str:
    return f"jwt:blocklist:{jti}"

def add_to_blocklist(jti: str, ttl_seconds: int) -> None:
    """Add a JWT ID to the blocklist. Expires after ttl_seconds."""
    if ttl_seconds > 0:
        _redis.setex(_blocklist_key(jti), ttl_seconds, "1")

def is_blocked(jti: str) -> bool:
    """Return True if the given jti is on the blocklist."""
    return _redis.exists(_blocklist_key(jti)) > 0

def reset_all() -> None:
    """Test-only: clear the blocklist between tests."""
    for k in _redis.scan_iter("jwt:blocklist:*"):
        _redis.delete(k)
