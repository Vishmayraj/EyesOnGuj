import uuid
import logging
import json
from typing import Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from shared.db.models import User as UserModel

logger = logging.getLogger(__name__)

# BUG-014 fix: cap details JSON at 64 KB to prevent oversized audit rows
# from crashing the INSERT and silently swallowing the audit trail.
_MAX_DETAILS_BYTES = 65536  # 64 KB


def log_audit_event(
    db: Session,
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
    user: Optional[UserModel] = None,
):
    """
    Stage a security-sensitive action into the audit_logs table.

    BUG-002 fix: This function no longer calls db.commit() itself.
    The caller MUST call db.flush() before this function and db.commit()
    after it, so the audit log INSERT and the primary action share a single
    transaction. If the audit INSERT fails, the primary action is also
    rolled back — preserving non-repudiation.

    Example (correct pattern):
        db.add(resource)
        db.flush()                      # stage primary INSERT — no commit yet
        log_audit_event(db, ...)        # stage audit INSERT — no commit yet
        db.commit()                     # ONE commit covers both
    """
    try:
        user_id = user.id if user else None
        department_id = user.department_id if user else None

        # BUG-014 fix: truncate oversized details to prevent INSERT failures
        details_json = json.dumps(details) if details else None
        if details_json and len(details_json.encode("utf-8")) > _MAX_DETAILS_BYTES:
            details_json = json.dumps({
                "truncated": True,
                "reason": "details payload exceeded 64 KB limit",
                "original_size_bytes": len(details_json.encode("utf-8")),
            })

        db.execute(text(
            """
            INSERT INTO audit_logs (id, action, resource_type, resource_id, details, user_id, department_id)
            VALUES (:id, :action, :resource_type, :resource_id, :details, :user_id, :department_id)
            """
        ), {
            "id": str(uuid.uuid4()),
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "details": details_json,
            "user_id": user_id,
            "department_id": department_id,
        })
        # NOTE: No db.commit() here — caller is responsible. See docstring above.
    except Exception as e:
        logger.error(f"Failed to stage audit log entry: {e}")
        # Re-raise so the caller's transaction is also rolled back,
        # rather than silently continuing without an audit trail.
        raise

