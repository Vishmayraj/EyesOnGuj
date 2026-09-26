"""
Auth API router — login and logout endpoints.
POST /api/v1/auth/login
POST /api/v1/auth/logout
"""

from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.rate_limit import login_rate_limiter, rate_limit_key
from app.auth.security import create_access_token, verify_password
from app.config import settings
from shared.db.models import User as UserModel
from shared.db.session import get_db

# secure=True refuses to send the cookie over plain HTTP at all - correct
# once infra/Caddyfile is terminating real TLS (AuditReport1.md finding
# 2.1), but it would break local http://localhost:8000 development if
# always on, since browsers silently drop "secure" cookies set over HTTP.
# settings.DEBUG is already the app's one existing prod/dev switch (see
# config.py's own SECRET_KEY fail-fast), so reuse it here instead of
# adding a second flag: DEBUG=True (local/test default) -> not secure,
# DEBUG=False (the docker-compose default) -> secure.
_COOKIE_SECURE = not settings.DEBUG

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class UserResponse(BaseModel):
    id: uuid.UUID
    username: str
    role: str
    department_id: Optional[uuid.UUID] = None


class LoginResponse(BaseModel):
    status: str
    user: UserResponse


@router.post("/login", response_model=LoginResponse)
def login(
    credentials: LoginRequest,
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    """Authenticate user with username and password, issuing an httpOnly JWT cookie."""
    client_ip = request.client.host if request.client else "unknown"
    
    # Trust X-Forwarded-For only if the request comes from a trusted proxy
    trusted_proxies = [p.strip() for p in settings.TRUSTED_PROXIES.split(",")]
    if client_ip in trusted_proxies:
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Take the first IP in the list (the original client)
            client_ip = forwarded_for.split(",")[0].strip()
            
    rl_key = rate_limit_key(client_ip, credentials.username)

    retry_after = login_rate_limiter.seconds_until_unlocked(rl_key)
    if retry_after > 0:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many failed login attempts. Try again later.",
            headers={"Retry-After": str(int(retry_after) + 1)},
        )

    user = (
        db.query(UserModel)
        .filter(UserModel.username == credentials.username)
        .first()
    )

    if not user or not user.is_active or not verify_password(credentials.password, user.hashed_password):
        login_rate_limiter.record_failure(rl_key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )

    login_rate_limiter.record_success(rl_key)

    token_data = {
        "sub": str(user.id),
        "username": user.username,
        "role": user.role,
        "department_id": str(user.department_id) if user.department_id else None,
    }
    access_token = create_access_token(token_data)

    csrf_token = str(uuid.uuid4())
    
    # Set httpOnly cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        samesite="lax",
        secure=_COOKIE_SECURE,
        path="/",
    )
    
    # Set CSRF cookie (NOT httpOnly so JS can read it and send it in headers)
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        samesite="lax",
        secure=_COOKIE_SECURE,
        path="/",
    )

    return {
        "status": "success",
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "department_id": user.department_id,
        },
    }


@router.post("/logout")
def logout(response: Response):
    """Log out current user by clearing the authentication cookie."""
    # Match the attributes the cookie was actually set with (samesite,
    # secure) - some browsers only clear a cookie via Set-Cookie when the
    # deleting response's attributes line up with how it was set.
    response.delete_cookie(
        key="access_token",
        path="/",
        samesite="lax",
        secure=_COOKIE_SECURE,
    )
    response.delete_cookie(
        key="csrf_token",
        path="/",
        samesite="lax",
        secure=_COOKIE_SECURE,
    )
    return {"status": "logged_out"}
