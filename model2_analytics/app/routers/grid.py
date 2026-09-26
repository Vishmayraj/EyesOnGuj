"""
Model 2 — Multi-Camera Live Grid Router & Ingestion Catalogue API

Endpoints:
  - GET  /grid                  (HTML View: Multi-camera control-room grid UI)
  - GET  /api/v1/grid/streams   (JSON API: List all camera streams with RTSP/WHEP/HLS URLs)
  - POST /api/v1/grid/sync      (JSON API: Sync/Ingest catalogue updates)
  - GET  /api/ingest            (JSON API: Ingestion catalogue contract endpoint)
"""

import uuid
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Request, HTTPException, status
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session, joinedload
from urllib.parse import urlparse

import socket
import ipaddress

def resolve_safe_url(url: Optional[str]) -> tuple[bool, Optional[str], Optional[str]]:
    if not url:
        return True, "", ""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ["http", "https", "rtsp"]:
            return False, "", ""
        host = parsed.hostname
        if not host:
            return False, "", ""
            
        ip = socket.gethostbyname(host)
        ip_obj = ipaddress.ip_address(ip)
        
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            return False, "", ""
        if str(ip_obj) == "169.254.169.254":
            return False, "", ""
            
        pinned_url = parsed._replace(netloc=f"{ip}:{parsed.port}" if parsed.port else ip).geturl()
        return True, pinned_url, host
    except Exception:
        return False, "", ""

def is_safe_url(url: Optional[str]) -> bool:
    safe, _, _ = resolve_safe_url(url)
    return safe

from app.auth.dependencies import get_current_user, require_role
from shared.db.models import User as UserModel

from shared.db.models import Camera as CameraModel
from shared.db.models import Department as DeptModel
from shared.db.models import District as DistModel
from shared.db.session import get_db
from shared.schemas.grid import (
    CameraStreamResponse,
    CatalogueSyncRequest,
    CatalogueSyncResponse,
    StreamProperties,
)

router = APIRouter(tags=["live-grid"])


def _format_cam_tag(source_id: str) -> str:
    """Normalize camera source id to standard cam01...cam30 format."""
    if not source_id:
        return "cam01"
    clean = str(source_id).strip().lower()
    if clean.startswith("cam") and clean[3:].isdigit():
        return f"cam{int(clean[3:]):02d}"
    if clean.isdigit():
        return f"cam{int(clean):02d}"
    return clean


def _build_stream_urls(cam: CameraModel) -> tuple[str, str, str]:
    """Helper to generate RTSP, WHEP, and HLS URLs for a camera pointing to the Sentinel Grid gateway."""
    from app.config import settings

    source_id = cam.source_grid_id or str(cam.id)[:8]
    cam_tag = _format_cam_tag(source_id)

    user = settings.GRID_RTSP_USER
    passwd = settings.GRID_RTSP_PASS
    host = settings.GRID_RTSP_HOST
    auth = f"{user}:{passwd}@" if user and passwd else ""

    rtsp = f"rtsp://{auth}{host}:{settings.GRID_RTSP_PORT}/stream/{cam_tag}"
    whep = f"http://{host}:{settings.GRID_WHEP_PORT}/stream/{cam_tag}/whep"
    hls = f"https://{settings.GRID_CDN_HOST}/{cam_tag}/index.m3u8"

    return rtsp, whep, hls


# ── REST API: Streams List ───────────────────────────────────────


@router.get("/api/v1/grid/streams", response_model=List[CameraStreamResponse])
def get_grid_streams(
    request: Request,
    department_id: Optional[uuid.UUID] = Query(None, description="Filter streams by department"),
    district_id: Optional[uuid.UUID] = Query(None, description="Filter streams by district"),
    connectivity_status: Optional[str] = Query(None, description="Filter by status (online/offline)"),
    is_live_only: bool = Query(False, description="Filter active live streams only"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Retrieve all camera feeds and their stream endpoints (RTSP, WHEP WebRTC, HLS) for video grid rendering.
    """
    query = (
        db.query(CameraModel)
        .options(joinedload(CameraModel.department), joinedload(CameraModel.district))
        .filter(CameraModel.is_active.is_(True))
    )

    if department_id:
        query = query.filter(CameraModel.department_id == department_id)
    if district_id:
        query = query.filter(CameraModel.district_id == district_id)
    if connectivity_status:
        query = query.filter(CameraModel.connectivity_status == connectivity_status)
    if is_live_only:
        query = query.filter(CameraModel.is_live.is_(True))

    cameras = query.order_by(CameraModel.name).limit(limit).offset(offset).all()
    results = []

    for cam in cameras:
        rtsp, whep, hls = _build_stream_urls(cam)
        results.append(
            CameraStreamResponse(
                id=cam.id,
                name=cam.name,
                department_name=cam.department.name if cam.department else None,
                district_name=cam.district.name if cam.district else None,
                connectivity_status=cam.connectivity_status,
                is_active=cam.is_active,
                is_live=cam.is_live or (cam.connectivity_status == "online"),
                source_grid_id=cam.source_grid_id or str(cam.id)[:8],
                location_label=cam.location_label or (cam.district.name if cam.district else "Gujarat"),
                vms_url=cam.vms_url,
                rtsp_url=rtsp,
                whep_url=whep,
                hls_url=hls,
                properties=StreamProperties(
                    codec=cam.codec or "H.264",
                    stream_width=cam.stream_width or 1920,
                    stream_height=cam.stream_height or 1080,
                    stream_fps=cam.stream_fps or 25.0,
                    bitrate_kbps=cam.bitrate_kbps or 2048,
                ),
            )
        )

    return results


# ── REST API: Catalogue Ingestion Contract (/api/ingest) ─────────


@router.get("/api/ingest")
def get_ingest_catalogue(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Hackathon Portal Ingestion Contract Endpoint.
    Returns every camera with its id, location, codec, live status, stream properties, and all 3 URLs.
    """
    cameras = db.query(CameraModel).filter(CameraModel.is_active.is_(True)).all()
    catalogue = []

    for cam in cameras:
        source_id = cam.source_grid_id or str(cam.id)[:8]
        rtsp, whep, hls = _build_stream_urls(cam)
        catalogue.append(
            {
                "id": source_id,
                "camera_uuid": str(cam.id),
                "name": cam.name,
                "location_label": cam.location_label or "Gujarat Site",
                "is_live": cam.is_live or (cam.connectivity_status == "online"),
                "codec": cam.codec or "H.264",
                "stream_width": cam.stream_width or 1920,
                "stream_height": cam.stream_height or 1080,
                "stream_fps": cam.stream_fps or 25.0,
                "bitrate_kbps": cam.bitrate_kbps or 2048,
                "rtsp_url": rtsp,
                "whep_url": whep,
                "hls_url": hls,
            }
        )

    return {"count": len(catalogue), "cameras": catalogue}


# ── REST API: Catalogue Sync ─────────────────────────────────────


@router.post("/api/v1/grid/sync", response_model=CatalogueSyncResponse)
def sync_ingest_catalogue(
    payload: CatalogueSyncRequest,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin")),
):
    """
    Sync camera streams from the ingestion catalogue into the database.
    """
    items = payload.items or []
    synced_count = 0
    updated_count = 0

    for item in items:
        if not is_safe_url(item.rtsp_url) or not is_safe_url(item.whep_url) or not is_safe_url(item.hls_url):
            raise HTTPException(status_code=400, detail=f"Invalid or unsafe URL provided for source_grid_id {item.id}")

        cam = db.query(CameraModel).filter(CameraModel.source_grid_id == item.id).first()
        if cam:
            cam.location_label = item.location_label or cam.location_label
            cam.is_live = item.is_live
            cam.codec = item.codec or cam.codec
            cam.stream_width = item.width or cam.stream_width
            cam.stream_height = item.height or cam.stream_height
            cam.stream_fps = item.fps or cam.stream_fps
            cam.bitrate_kbps = item.bitrate_kbps or cam.bitrate_kbps
            cam.rtsp_url = item.rtsp_url or cam.rtsp_url
            cam.whep_url = item.whep_url or cam.whep_url
            cam.hls_url = item.hls_url or cam.hls_url
            updated_count += 1
        synced_count += 1

    db.commit()
    return CatalogueSyncResponse(
        synced_count=synced_count,
        updated_count=updated_count,
        status="success",
    )
