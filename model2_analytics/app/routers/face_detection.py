"""
Model 2 — Face Detection & Person Watchlist REST and WebSocket Router.
======================================================================
Provides endpoints for:
  - Video upload & OpenCV metadata probing.
  - FaceVideoWorker job lifecycle management (start, pause, resume, stop).
  - Streaming real-time video frames and suspect match alerts over WebSockets.
  - Querying person alerts history from PostgreSQL.
  - Secure serving of detected face match crops.
"""

import asyncio
import json
import logging
import re
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set

import cv2
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.auth.dependencies import get_current_user, require_role
from pipeline.face_detection.worker import FaceVideoWorker
from shared.db.models import (
    Camera as CameraModel,
    PersonAlert as PersonAlertModel,
    PersonWatchlist as PersonWatchlistModel,
    User as UserModel,
)
from shared.db.session import get_db

logger = logging.getLogger("sentinel.face_detection.router")
logger.setLevel(logging.INFO)

router = APIRouter(prefix="/api/v1/face-detection", tags=["face-detection"])

# ── Upload directory resolution ──────────────────────────────────
UPLOADS_DIR = Path("/app/model2_analytics/uploads/videos")
if not UPLOADS_DIR.exists():
    UPLOADS_DIR = Path(__file__).resolve().parents[2] / "uploads" / "videos"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

CROPS_DIR = Path("/app/model2_analytics/uploads/face_matches")
if not CROPS_DIR.exists():
    CROPS_DIR = Path(__file__).resolve().parents[2] / "uploads" / "face_matches"
CROPS_DIR.mkdir(parents=True, exist_ok=True)

MAX_UPLOAD_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB
ALLOWED_EXTENSIONS = {".mp4", ".avi", ".mov", ".mkv", ".webm"}

# ── Module-level state & WebSocket hub ───────────────────────────
_JOBS: Dict[str, FaceVideoWorker] = {}
_JOBS_META: Dict[str, Dict] = {}
_JOB_WS: Dict[str, Set[WebSocket]] = defaultdict(set)
_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_db_session():
    import shared.db.session as _s
    return _s._SessionLocal() if _s._SessionLocal else None


def _capture_running_loop():
    global _loop
    try:
        _loop = asyncio.get_running_loop()
    except RuntimeError:
        pass


# ── Thread-safe event callback from FaceVideoWorker ───────────────
def on_face_worker_event(payload: Dict):
    data = payload.get("data", {})
    job_id = data.get("job_id")
    if not job_id:
        return

    global _loop
    loop = _loop
    if loop is not None and not loop.is_closed() and loop.is_running():
        asyncio.run_coroutine_threadsafe(_broadcast_job_event(job_id, payload), loop)
    else:
        logger.warning(f"[{job_id}] Event loop not active - cannot broadcast event {payload.get('type')}")


async def _broadcast_job_event(job_id: str, payload: Dict):
    clients = _JOB_WS.get(job_id, set())
    if not clients:
        return

    dead: Set[WebSocket] = set()
    text_data = json.dumps(payload)
    for ws in list(clients):
        try:
            await ws.send_text(text_data)
        except Exception:
            dead.add(ws)

    if dead:
        clients.difference_update(dead)


# ── Request Schemas ───────────────────────────────────────────────
class JobControlRequest(BaseModel):
    job_id: str
    speed: Optional[str] = "1x"


# ── 1. Cameras for Association ────────────────────────────────────
@router.get("/cameras")
def get_cameras_for_association(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """List cameras available to associate with surveillance video footage."""
    cams = (
        db.query(CameraModel)
        .filter(CameraModel.is_active == True)
        .order_by(CameraModel.name)
        .all()
    )
    return [
        {
            "id": str(c.id),
            "name": c.name,
            "district": c.district.name if c.district else None,
            "department": c.department.name if c.department else None,
        }
        for c in cams
    ]


# ── 2. Video Upload Endpoint ──────────────────────────────────────
@router.post("/upload")
async def upload_face_video(
    file: UploadFile = File(..., description="Video file (.mp4, .avi, .mov, etc.)"),
    camera_id: Optional[str] = Form(None, description="Optional camera ID or defaults to 'prerecorded'"),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Upload surveillance video footage, probe metadata via OpenCV,
    and initialize job record with camera_id stored as 'prerecorded'.
    """
    _capture_running_loop()

    filename = file.filename or "footage.mp4"
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported format '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )

    # Resolve camera_id: Always store 'prerecorded' in schema for user recorded video uploads
    stored_camera_id = "prerecorded"
    camera_name = "Pre-recorded Video Upload"
    if camera_id and camera_id.strip() and camera_id != "prerecorded":
        try:
            cam_uuid = uuid.UUID(camera_id)
            cam = db.query(CameraModel).filter(CameraModel.id == cam_uuid).first()
            if cam:
                camera_name = f"Pre-recorded ({cam.name})"
        except ValueError:
            pass

    job_id = str(uuid.uuid4())
    clean_filename = re.sub(r"[^\w\-.]", "_", filename)
    target_path = UPLOADS_DIR / f"{job_id}_{clean_filename}"

    # Stream chunks with 2 GB limit
    total_bytes = 0
    try:
        with open(target_path, "wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > MAX_UPLOAD_SIZE:
                    target_path.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail="Video file exceeds maximum permitted size of 2 GB",
                    )
                buffer.write(chunk)
    except Exception as e:
        target_path.unlink(missing_ok=True)
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"Failed to write video: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to write video file: {str(e)}")

    # Probe video metadata using OpenCV
    cap = cv2.VideoCapture(str(target_path))
    if not cap.isOpened():
        target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="Could not decode video file (corrupted or unreadable format)")

    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = round(float(cap.get(cv2.CAP_PROP_FPS)) or 25.0, 2)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration_sec = round(total_frames / fps, 2) if fps > 0 else 0
    cap.release()

    _JOBS_META[job_id] = {
        "job_id": job_id,
        "camera_id": stored_camera_id,
        "camera_name": camera_name,
        "file_path": str(target_path),
        "filename": filename,
        "width": width,
        "height": height,
        "fps": fps,
        "total_frames": total_frames,
        "duration_sec": duration_sec,
        "file_size_bytes": total_bytes,
        "uploaded_by": current_user.username,
        "state": "ready",
    }

    return {
        "job_id": job_id,
        "filename": filename,
        "camera_id": stored_camera_id,
        "camera_name": camera_name,
        "metadata": {
            "resolution": f"{width}x{height}",
            "fps": fps,
            "total_frames": total_frames,
            "duration_sec": duration_sec,
            "size_mb": round(total_bytes / (1024 * 1024), 2),
        },
    }


# ── 3. Start Face Detection Job ───────────────────────────────────
@router.post("/start")
async def start_face_job(
    payload: JobControlRequest,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """Start asynchronous face detection & watchlist matching worker."""
    _capture_running_loop()

    job_id = payload.job_id
    meta = _JOBS_META.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Job not found or expired")
        
    if current_user.role != "dept_admin" and meta.get("uploaded_by") != current_user.username:
        raise HTTPException(status_code=403, detail="You can only control jobs you uploaded.")

    worker = _JOBS.get(job_id)
    if worker and worker.is_running:
        return {"status": "already_running", "job_id": job_id}

    worker = FaceVideoWorker(
        job_id=job_id,
        file_path=meta["file_path"],
        camera_id=meta["camera_id"],
        camera_name=meta["camera_name"],
        speed=payload.speed or "1x",
        event_callback=on_face_worker_event,
        db_session_factory=_get_db_session,
    )
    _JOBS[job_id] = worker
    worker.start()
    meta["state"] = "running"

    return {
        "status": "started",
        "job_id": job_id,
        "camera_id": meta["camera_id"],
        "camera_name": meta["camera_name"],
        "speed": payload.speed or "1x",
    }


# ── 4. Pause Face Detection Job ───────────────────────────────────
@router.post("/pause")
async def pause_face_job(
    payload: JobControlRequest,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """Pause live processing."""
    _capture_running_loop()
    worker = _JOBS.get(payload.job_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Job not found")

    meta = _JOBS_META.get(payload.job_id)
    if meta and current_user.role != "dept_admin" and meta.get("uploaded_by") != current_user.username:
        raise HTTPException(status_code=403, detail="You can only control jobs you uploaded.")
    worker.pause()
    return {"status": "paused", "job_id": payload.job_id}


# ── 5. Resume Face Detection Job ──────────────────────────────────
@router.post("/resume")
async def resume_face_job(
    payload: JobControlRequest,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """Resume paused processing."""
    _capture_running_loop()
    worker = _JOBS.get(payload.job_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Job not found")

    meta = _JOBS_META.get(payload.job_id)
    if meta and current_user.role != "dept_admin" and meta.get("uploaded_by") != current_user.username:
        raise HTTPException(status_code=403, detail="You can only control jobs you uploaded.")
    worker.resume()
    return {"status": "resumed", "job_id": payload.job_id}


# ── 6. Stop Face Detection Job ────────────────────────────────────
@router.post("/stop")
async def stop_face_job(
    payload: JobControlRequest,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """Gracefully terminate worker thread."""
    _capture_running_loop()
    worker = _JOBS.get(payload.job_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Job not found")

    meta = _JOBS_META.get(payload.job_id)
    if meta and current_user.role != "dept_admin" and meta.get("uploaded_by") != current_user.username:
        raise HTTPException(status_code=403, detail="You can only control jobs you uploaded.")
    worker.stop()
    return {"status": "stopped", "job_id": payload.job_id}


# ── 7. Status Query Endpoint ──────────────────────────────────────
@router.get("/status/{job_id}")
def get_face_job_status(
    job_id: str,
    current_user: UserModel = Depends(get_current_user),
):
    meta = _JOBS_META.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Job not found")

    worker = _JOBS.get(job_id)
    if worker:
        return {
            "job_id": job_id,
            "state": worker.state,
            "current_frame": worker.current_frame,
            "total_frames": worker.total_frames,
            "progress": round((worker.current_frame / max(1, worker.total_frames)) * 100, 1),
            "processing_fps": round(worker.processing_fps, 1),
            "total_faces": worker.total_faces_detected,
            "total_matches": worker.total_matches,
        }

    return {"job_id": job_id, "state": meta.get("state", "ready")}


@router.delete("/{job_id}")
def delete_face_job(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Cancel a job, remove it from memory, and delete the uploaded media file from disk.
    """
    meta = _JOBS_META.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Job not found")

    if current_user.role != "dept_admin" and meta.get("uploaded_by") != current_user.username:
        raise HTTPException(status_code=403, detail="You can only delete jobs you uploaded.")

    # Stop the worker if running
    worker = _JOBS.get(job_id)
    if worker:
        worker.stop()
        del _JOBS[job_id]

    # Delete physical file
    target_path = Path(meta.get("file_path") or meta.get("saved_path"))
    target_path.unlink(missing_ok=True)

    del _JOBS_META[job_id]

    return {"status": "success", "job_id": job_id, "action": "deleted"}


# ── 8. Query Person Alerts History ────────────────────────────────
@router.get("/alerts")
def get_face_alerts(
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Retrieve paginated person watchlist match alerts."""
    alerts = (
        db.query(PersonAlertModel)
        .order_by(desc(PersonAlertModel.created_at))
        .offset(offset)
        .limit(limit)
        .all()
    )

    out = []
    for a in alerts:
        person = a.person
        out.append({
            "id": str(a.id),
            "person_id": str(a.person_id),
            "name": person.name if person else "Unknown",
            "category": person.category if person else "wanted",
            "watchlist_photo": person.photo_path if person else None,
            "similarity": round(a.similarity_score, 4),
            "distance": round(a.distance, 4),
            "crop_url": a.face_crop_path,
            "camera_name": a.camera_id if a.camera_id else "Pre-recorded Video",
            "timestamp": a.created_at.isoformat() if a.created_at else None,
            "acknowledged": a.acknowledged_at is not None,
        })
    return out


# ── 9. Crop Serving with Path Traversal Defense ───────────────────
@router.get("/crops/{filename}")
def get_face_crop(
    filename: str,
    current_user: UserModel = Depends(get_current_user),
):
    """Serve detected face crop thumbnails securely."""
    safe_filename = Path(filename).name
    crop_path = (CROPS_DIR / safe_filename).resolve()

    if not crop_path.is_relative_to(CROPS_DIR.resolve()) or not crop_path.exists():
        raise HTTPException(status_code=404, detail="Face crop not found")

    return FileResponse(str(crop_path), media_type="image/jpeg")


# ── 10. Real-time WebSocket Endpoint ──────────────────────────────
@router.websocket("/ws/{job_id}")
async def ws_face_stream(websocket: WebSocket, job_id: str):
    """
    Streams VIDEO_FRAME, PERSON_MATCH alerts, and JOB_PROGRESS events to clients.
    """
    _capture_running_loop()

    # Authenticate token safely from cookie, header, or query param
    token = websocket.cookies.get("access_token")
    if token and token.startswith("Bearer "):
        token = token[7:]
    if not token:
        auth_header = websocket.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        token = websocket.query_params.get("token")
        if token and token.startswith("Bearer "):
            token = token[7:]

    user = None
    if token:
        try:
            from app.auth.security import decode_access_token
            payload = decode_access_token(token)
            if payload and "sub" in payload:
                user_id = uuid.UUID(payload["sub"])
                db = _get_db_session()
                if db:
                    try:
                        user = db.query(UserModel).filter(UserModel.id == user_id, UserModel.is_active.is_(True)).first()
                    finally:
                        db.close()
        except Exception as e:
            logger.debug(f"Face WS auth error: {e}")
            user = None

    if not user:
        logger.warning(f"[{job_id}] Unauthenticated WebSocket connection to /ws/{job_id} — rejecting cleanly.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Unauthorized")
        return

    await websocket.accept()
    _JOB_WS[job_id].add(websocket)
    logger.info(f"WebSocket client connected to job {job_id}")

    try:
        while True:
            # Keepalive listener
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _JOB_WS[job_id].discard(websocket)
        logger.info(f"WebSocket client disconnected from job {job_id}")
