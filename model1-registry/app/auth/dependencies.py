"""
Authentication and RBAC dependencies for FastAPI routers.
"""

from typing import Callable, Optional
import uuid

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.security import decode_access_token
from app.auth.token_blocklist import is_blocked
from shared.db.models import User as UserModel
from shared.db.session import get_db


def get_token_from_request(request: Request) -> Optional[str]:
    """Extract JWT token from httpOnly cookie or Bearer Authorization header."""
    # 1. Cookie check
    cookie_token = request.cookies.get("access_token")
    if cookie_token:
        if cookie_token.startswith("Bearer "):
            return cookie_token[7:]
        return cookie_token

    # 2. Authorization header check
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header[7:]

    return None


def get_current_user(
    request: Request, db: Session = Depends(get_db)
) -> UserModel:
    """Dependency that returns the authenticated User or raises HTTP 401."""
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token missing or invalid.",
        )

    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication token.",
        )

    # BUG-010 fix: reject tokens that were explicitly revoked at logout
    jti = payload.get("jti")
    if jti and is_blocked(jti):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has been revoked. Please log in again.",
        )

    user_id_str = payload.get("sub")
    try:
        user_id = uuid.UUID(user_id_str)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID in token.",
        )

    user = db.query(UserModel).filter(UserModel.id == user_id).first()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or account deactivated.",
        )

    # CSRF Protection: Enforce on state-changing methods if using cookie auth
    if getattr(request, "method", None) in ["POST", "PUT", "PATCH", "DELETE"]:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            # Client relies on cookies, so enforce CSRF
            csrf_cookie = request.cookies.get("csrf_token")
            csrf_header = request.headers.get("x-csrf-token")
            if not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="CSRF token missing or invalid.",
                )

    return user


def get_optional_current_user(
    request: Request, db: Session = Depends(get_db)
) -> Optional[UserModel]:
    """Returns current authenticated User if session is valid, otherwise None."""
    token = get_token_from_request(request)
    if not token:
        return None

    payload = decode_access_token(token)
    if not payload or "sub" not in payload:
        return None

    user_id_str = payload.get("sub")
    try:
        user_id = uuid.UUID(user_id_str)
        user = db.query(UserModel).filter(UserModel.id == user_id).first()
        if user and user.is_active:
            return user
    except Exception:
        return None

    return None


def require_role(*allowed_roles: str) -> Callable:
    """Dependency factory enforcing role membership.
    
    Usage: Depends(require_role('dept_admin')) or Depends(require_role('dept_admin', 'operator'))
    """
    def dependency(current_user: UserModel = Depends(get_current_user)) -> UserModel:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{current_user.role}' is not authorized to perform this action.",
            )
        return current_user

    return dependency
