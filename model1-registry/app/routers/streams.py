"""
Stream Router — Universal, high-performance live stream broadcaster.

Decodes both H.264 and HEVC (H.265) feeds server-side via OpenCV/FFmpeg
and streams live multipart/x-mixed-replace JPEG frames directly to web dashboards.
Eliminates browser-side codec (HEVC) incompatibilities and WebRTC UDP firewall blocks.

Also exposes /api/v1/streams/catalogue — a dynamic JSON catalogue of all live camera
stream endpoints (HLS, RTSP, MJPEG) read from DB, with fallback to the known grid.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from typing import Dict, List, Optional
from urllib.parse import quote
import uuid

import cv2
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse, Response
import numpy as np
from sqlalchemy.orm import Session

from shared.db.models import Camera as CameraModel
from shared.db.models import User as UserModel
from shared.db.session import get_db
from app.auth.dependencies import get_current_user
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/cameras", tags=["live-streams"])

# ── Separate router for /api/v1/streams/* ──────────────────────────────
streams_router = APIRouter(prefix="/api/v1/streams", tags=["streams"])

os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"


def _create_placeholder_jpeg(text: str) -> bytes:
    img = np.zeros((360, 640, 3), dtype=np.uint8)
    img[:] = (15, 23, 42)  # Dark slate background
    cv2.putText(
        img,
        text,
        (160, 180),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (148, 163, 184),
        2,
        cv2.LINE_AA,
    )
    ret, buf = cv2.imencode(".jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
    return buf.tobytes() if ret else b""


class CameraStreamReader:
    """
    Manages an on-demand RTSP/HLS stream reader for one camera.
    Maintains a single background thread while clients are actively viewing.
    Automatically disconnects when no clients have requested frames for > 10 seconds.
    """

    def __init__(self, rtsp_url: str, cam_id: str) -> None:
        self.rtsp_url = rtsp_url
        self.cam_id = cam_id
        self._lock = threading.Lock()
        self._latest_jpeg: bytes = _create_placeholder_jpeg(f"{cam_id.upper()} Connecting...")
        self._last_access_time: float = time.time()
        self._active_viewers: int = 0
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None

    def add_viewer(self) -> None:
        with self._lock:
            self._active_viewers += 1
            self._last_access_time = time.time()
            if not self._running:
                self._running = True
                self._thread = threading.Thread(
                    target=self._capture_loop,
                    name=f"stream-reader-{self.cam_id}",
                    daemon=True,
                )
                self._thread.start()

    def touch(self) -> None:
        with self._lock:
            self._last_access_time = time.time()
            if not self._running:
                self._running = True
                self._thread = threading.Thread(
                    target=self._capture_loop,
                    name=f"stream-reader-{self.cam_id}",
                    daemon=True,
                )
                self._thread.start()

    def remove_viewer(self) -> None:
        with self._lock:
            self._active_viewers = max(0, self._active_viewers - 1)

    def get_latest_jpeg(self) -> bytes:
        with self._lock:
            self._last_access_time = time.time()
            return self._latest_jpeg

    def _capture_loop(self) -> None:
        # Force TCP and zero buffer to prevent frame queue lag
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|fflags;nobuffer|max_delay;500000"
        logger.info(f"[{self.cam_id}] Starting optimized on-demand RTSP capture: {self.rtsp_url}")
        cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), 55]

        consecutive_failures = 0

        while self._running:
            with self._lock:
                if self._active_viewers <= 0 and (time.time() - self._last_access_time > 4.0):
                    logger.info(f"[{self.cam_id}] Inactive timeout, closing stream.")
                    self._running = False
                    break

            if not cap.isOpened():
                time.sleep(0.8)
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|fflags;nobuffer|max_delay;500000"
                cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
                continue

            ok, frame = cap.read()
            if not ok:
                consecutive_failures += 1
                # Remote grid cameras (e.g. cam07-cam30) have GOP keyframe intervals up to 10-12s.
                # Do NOT reconnect after 0.25s; allow up to 60 read attempts (~9 seconds) for keyframe.
                if consecutive_failures > 60:
                    logger.warning(f"[{self.cam_id}] Stream read failed for >9s, reconnecting...")
                    cap.release()
                    time.sleep(1.0)
                    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|fflags;nobuffer|max_delay;500000"
                    cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
                    consecutive_failures = 0
                else:
                    time.sleep(0.12)
                continue

            consecutive_failures = 0

            # Downsample 1080p to smooth 640x360 grid resolution (89% bandwidth & CPU reduction)
            if frame.shape[1] > 640:
                h = int(frame.shape[0] * 640 / frame.shape[1])
                frame = cv2.resize(frame, (640, h), interpolation=cv2.INTER_AREA)

            # Encode frame to optimized JPEG
            ret, buffer = cv2.imencode(".jpg", frame, encode_params)
            if ret:
                with self._lock:
                    self._latest_jpeg = buffer.tobytes()

            time.sleep(0.06)  # ~16 fps capture pace to eliminate CPU saturation

        if cap.isOpened():
            cap.release()
        logger.info(f"[{self.cam_id}] Stream capture cleanly stopped.")


# Global stream registry
_STREAM_READERS: Dict[str, CameraStreamReader] = {}
_REGISTRY_LOCK = threading.Lock()


def _build_authenticated_rtsp_url(stream_id: str) -> str:
    """
    Builds the internal RTSP connection URL with properly percent-encoded credentials (RFC 3986).
    Protects against special characters (like '@' in email usernames) breaking the URI scheme.
    """
    if settings.GRID_RTSP_USER and settings.GRID_RTSP_PASS:
        user_enc = quote(settings.GRID_RTSP_USER, safe="")
        pass_enc = quote(settings.GRID_RTSP_PASS, safe="")
        return f"rtsp://{user_enc}:{pass_enc}@{settings.GRID_RTSP_HOST}:{settings.GRID_RTSP_PORT}/stream/{stream_id}"
    return f"rtsp://{settings.GRID_RTSP_HOST}:{settings.GRID_RTSP_PORT}/stream/{stream_id}"


def get_or_create_stream_reader(cam_id: str, rtsp_url: str) -> CameraStreamReader:
    with _REGISTRY_LOCK:
        if cam_id not in _STREAM_READERS:
            _STREAM_READERS[cam_id] = CameraStreamReader(rtsp_url=rtsp_url, cam_id=cam_id)
        else:
            reader = _STREAM_READERS[cam_id]
            if reader.rtsp_url != rtsp_url:
                with reader._lock:
                    reader.rtsp_url = rtsp_url
                    reader._running = False
                _STREAM_READERS[cam_id] = CameraStreamReader(rtsp_url=rtsp_url, cam_id=cam_id)
        return _STREAM_READERS[cam_id]


async def frame_generator(reader: CameraStreamReader):
    reader.add_viewer()
    try:
        while True:
            jpeg = reader.get_latest_jpeg()
            yield (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n"
            )
            await asyncio.sleep(0.065)  # ~15 fps delivery to client for ultra-smooth matrix
    except (asyncio.CancelledError, Exception):
        pass
    finally:
        reader.remove_viewer()


@router.get("/grid/{grid_id}/frame")
async def get_camera_frame_by_grid_id(
    grid_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns the single latest JPEG frame for a camera.
    Ultra-lightweight endpoint used by matrix cards to prevent browser socket exhaustion.
    """
    clean_id = grid_id.lower()
    if clean_id.isdigit():
        clean_id = f"cam{int(clean_id):02d}"
    elif not clean_id.startswith("cam"):
        clean_id = f"cam{clean_id}"

    if current_user.department_id:
        camera = db.query(CameraModel).filter(CameraModel.source_grid_id == clean_id).first()
        if camera and camera.department_id != current_user.department_id:
            raise HTTPException(status_code=403, detail="Not authorized to view this camera")

    rtsp_url = _build_authenticated_rtsp_url(clean_id)

    reader = get_or_create_stream_reader(cam_id=clean_id, rtsp_url=rtsp_url)
    reader.touch()
    jpeg = reader.get_latest_jpeg()

    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.get("/grid/{grid_id}/live")
async def stream_camera_by_grid_id(
    grid_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Streams live video frames directly for a government grid camera (e.g. cam01 through cam30).
    Decodes both H.264 and HEVC (H.265) server-side with zero browser codec dependencies.
    """
    clean_id = grid_id.lower()
    if clean_id.isdigit():
        clean_id = f"cam{int(clean_id):02d}"
    elif not clean_id.startswith("cam"):
        clean_id = f"cam{clean_id}"

    if current_user.department_id:
        camera = db.query(CameraModel).filter(CameraModel.source_grid_id == clean_id).first()
        if camera and camera.department_id != current_user.department_id:
            raise HTTPException(status_code=403, detail="Not authorized to view this camera stream")

    rtsp_url = _build_authenticated_rtsp_url(clean_id)

    reader = get_or_create_stream_reader(cam_id=clean_id, rtsp_url=rtsp_url)

    return StreamingResponse(
        frame_generator(reader),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@router.get("/{camera_id}/live")
async def stream_camera_by_uuid(
    camera_id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Streams live video frames for a database camera row.
    """
    # Was reaching for shared.db.session._SessionLocal directly inside a
    # manual `with` block - a "private," underscore-prefixed module
    # attribute - instead of the Depends(get_db) pattern every other
    # endpoint in this file (and the whole codebase) uses. No behavior
    # change: get_db() raises the same RuntimeError if the engine hasn't
    # been initialised yet, and FastAPI closes the session for us on the
    # way out exactly like the old `with` block did (AuditReport1.md
    # finding 22 / 4.4).
    camera = db.query(CameraModel).filter(CameraModel.id == camera_id).first()
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found")

    if current_user.department_id and camera.department_id != current_user.department_id:
        raise HTTPException(status_code=403, detail="Not authorized to view this camera stream")

    rtsp_url = camera.rtsp_url
    grid_id = camera.source_grid_id or "cam01"

    if not rtsp_url:
        if grid_id.isdigit():
            grid_id = f"cam{int(grid_id):02d}"
        rtsp_url = _build_authenticated_rtsp_url(grid_id)

    reader = get_or_create_stream_reader(
        cam_id=str(camera_id),
        rtsp_url=rtsp_url,
    )

    return StreamingResponse(
        frame_generator(reader),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ── Stream Catalogue API ─────────────────────────────────────────────────


@streams_router.get("/catalogue")
async def get_stream_catalogue(
    request: Request,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Returns the live camera stream catalogue for the /live grid view.
    Always returns sanitized, public URLs (passwords protected in backend config).
    """
    raw_base = str(request.base_url)
    base_url = raw_base if raw_base.endswith("/") else f"{raw_base}/"

    q = db.query(CameraModel).filter(
        CameraModel.is_active == True,  # noqa: E712
        CameraModel.source_grid_id != None,  # noqa: E711
    )
    if current_user.department_id:
        q = q.filter(CameraModel.department_id == current_user.department_id)
        
    cameras = q.order_by(CameraModel.source_grid_id).all()

    if cameras:
        entries = []
        seen_ids = set()
        for cam in cameras:
            grid_id = cam.source_grid_id
            # Normalise grid_id to cam## format
            if grid_id and grid_id.isdigit():
                grid_id = f"cam{int(grid_id):02d}"
            elif grid_id and not grid_id.startswith("cam"):
                grid_id = f"cam{grid_id}"

            if not grid_id or grid_id in seen_ids:
                continue
            seen_ids.add(grid_id)

            public_rtsp = f"rtsp://{settings.GRID_RTSP_HOST}:{settings.GRID_RTSP_PORT}/stream/{grid_id}"
            entries.append(
                {
                    "id": grid_id,
                    "db_id": str(cam.id),
                    "name": cam.name or f"Camera {grid_id[3:] if len(grid_id) > 3 else grid_id}",
                    "hls_url": f"https://{settings.GRID_CDN_HOST}/{grid_id}/index.m3u8",
                    "mjpeg_url": f"{base_url}api/v1/cameras/grid/{grid_id}/live",
                    "frame_url": f"{base_url}api/v1/cameras/grid/{grid_id}/frame",
                    "rtsp_url": public_rtsp,
                    "whep_url": f"http://{settings.GRID_RTSP_HOST}:{settings.GRID_WHEP_PORT}/stream/{grid_id}/whep",
                    "codec": cam.codec or "h264",
                    "is_live": cam.is_live if cam.is_live is not None else True,
                    "location": cam.location_label or f"Ahmedabad — {grid_id}",
                    "department": cam.department.name if cam.department else "Home Department",
                }
            )
        entries.sort(key=lambda x: int(x["id"].replace("cam", "")) if x["id"].replace("cam", "").isdigit() else 999)
        return JSONResponse(content={"cameras": entries, "source": "db", "total": len(entries)})

    if current_user.department_id:
        return JSONResponse(content={"cameras": [], "source": "db", "total": 0})

    # Fallback: generate catalogue from public grid spec
    logger.info("DB has no grid-synced cameras — returning grid fallback catalogue")
    fallback = []
    for i in range(1, 31):
        grid_id = f"cam{i:02d}"
        public_rtsp = f"rtsp://{settings.GRID_RTSP_HOST}:{settings.GRID_RTSP_PORT}/stream/{grid_id}"
        fallback.append(
            {
                "id": grid_id,
                "db_id": None,
                "name": f"Camera {i:02d}",
                "hls_url": f"https://{settings.GRID_CDN_HOST}/{grid_id}/index.m3u8",
                "mjpeg_url": f"{base_url}api/v1/cameras/grid/{grid_id}/live",
                "frame_url": f"{base_url}api/v1/cameras/grid/{grid_id}/frame",
                "rtsp_url": public_rtsp,
                "whep_url": f"http://{settings.GRID_RTSP_HOST}:{settings.GRID_WHEP_PORT}/stream/{grid_id}/whep",
                "codec": "h264",
                "is_live": True,
                "location": f"Ahmedabad — {grid_id}",
                "department": "Home Department",
            }
        )

    return JSONResponse(
        content={"cameras": fallback, "source": "grid_fallback", "total": len(fallback)}
    )
