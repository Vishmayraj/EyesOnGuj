"""
App configuration via pydantic-settings.

Reads from environment variables (or a .env file if present).
"""

import os
from pydantic_settings import BaseSettings

# This value only exists so that local development (and the test suite)
# works out of the box without requiring a .env file. It is publicly
# visible in this checked-in source file, so it must never be allowed to
# sign real sessions - see the fail-fast check below Settings().
_INSECURE_DEFAULT_SECRET_KEY = "sentinel-secret-key-hackathon-2026-secure"


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://sentinel:sentinel_dev@127.0.0.1:5432/sentinel"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    DEBUG: bool = True
    SECRET_KEY: str = _INSECURE_DEFAULT_SECRET_KEY
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60   # BUG-009 fix: reduced from 480 (8h) to 60 minutes

    # Login rate limiting / lockout (AuditReport1.md finding 2.2). Keyed on
    # (client IP, username) - see app/auth/rate_limit.py.
    LOGIN_MAX_ATTEMPTS: int = 5
    LOGIN_WINDOW_SECONDS: int = 300    # 5 minutes: failures older than this don't count
    LOGIN_LOCKOUT_SECONDS: int = 900   # 15 minutes: how long a lockout lasts once triggered

    # VMS ingestion settings
    GRID_HOST: str = "cctv.corp8.cloud"
    MEDIAMTX_API: str = "localhost:9997"
    # Turns off RTSP/MediaMTX stream registration while leaving the
    # camera-catalogue poll itself running (see app/main.py's lifespan()
    # for the full explanation of what this does and doesn't disable).
    # Used to be read via a bare os.environ.get() in main.py instead of
    # going through this Settings object like every other env-driven
    # value in the app - meaning it wasn't validated, wasn't documented
    # in .env.example, and wouldn't show up alongside the rest of the
    # app's config if someone went looking for what's configurable
    # (AuditReport1.md finding 18).
    DISABLE_INGESTION: bool = False

    # Operational Sentinel Camera Grid gateway settings (configurable via env vars)
    GRID_RTSP_HOST: str = "103.250.160.189"  # Public static IP for direct RTSP & WebRTC
    GRID_RTSP_PORT: int = 8554               # Gateway RTSP port (TCP forced)
    GRID_RTSP_USER: str = os.getenv("GRID_RTSP_USER", "")
    GRID_RTSP_PASS: str = os.getenv("GRID_RTSP_PASS", "")
    GRID_WHEP_PORT: int = 8889               # Gateway WHEP WebRTC signaling port
    GRID_CDN_HOST: str = "cctv.corp8.cloud"  # CDN host for HLS

    # Model 3 — Federation event bus & GIS basemaps
    REDIS_URL: str = "redis://localhost:6379"
    CARTO_API_KEY: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()

if settings.SECRET_KEY == _INSECURE_DEFAULT_SECRET_KEY and not settings.DEBUG:
    # This key signs every JWT (see auth/security.py). Anyone who has read
    # this file on GitHub knows the default value, so silently signing
    # real sessions with it would let them mint a valid token for any
    # user/role on any deployment that forgot to override it. DEBUG=True
    # (the local-dev / test default) is treated as an explicit opt-in to
    # the insecure default; anything else must set a real SECRET_KEY.
    raise RuntimeError(
        "SECRET_KEY is unset (or still equals the public, checked-in "
        "default) while DEBUG=False. Refusing to start: set a real, "
        "secret SECRET_KEY via the environment before running outside "
        "local development. See .env.example."
    )

