"""
Camera API router — full CRUD + bulk CSV import.

Implements docs/API_Contract.md §1 endpoints:
  GET    /api/v1/cameras          — list / filter
  POST   /api/v1/cameras          — create (manual entry)
  POST   /api/v1/cameras/bulk     — CSV bulk import
  GET    /api/v1/cameras/{id}     — detail
  PATCH  /api/v1/cameras/{id}     — partial update
  DELETE /api/v1/cameras/{id}     — soft delete (is_active = false)
  GET    /api/v1/cameras/{id}/history — audit trail
"""

import csv
import io
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from geoalchemy2.shape import to_shape
from sqlalchemy import func, text
from sqlalchemy.orm import Session, joinedload

from shared.db.models import Camera as CameraModel
from shared.db.models import StatusHistory as StatusHistoryModel
from shared.db.session import get_db
from shared.schemas.camera import (
    BulkImportResult,
    Camera as CameraSchema,
    CameraCreate,
    CameraUpdate,
    GeoJSONPoint,
)

from app.auth.dependencies import get_current_user, require_role
from shared.db.models import User as UserModel

router = APIRouter(prefix="/api/v1/cameras", tags=["cameras"])


# ── helpers ─────────────────────────────────────────────────────


def _camera_to_schema(cam: CameraModel) -> CameraSchema:
    """Convert an ORM Camera (with joined department/district) to the
    Pydantic read schema, translating the PostGIS geography to GeoJSON."""
    location = None
    if cam.location is not None:
        try:
            point = to_shape(cam.location)
            location = GeoJSONPoint(coordinates=[point.x, point.y])
        except Exception:
            location = None

    return CameraSchema(
        id=cam.id,
        name=cam.name,
        department_id=cam.department_id,
        department_name=cam.department.name if cam.department else None,
        district_id=cam.district_id,
        district_name=cam.district.name if cam.district else None,
        location=location,
        location_label=cam.location_label,
        camera_type=cam.camera_type,
        ownership=cam.ownership,
        connectivity_status=cam.connectivity_status,
        storage_type=cam.storage_type,
        retention_days=cam.retention_days,
        vms_url=cam.vms_url,
        is_active=cam.is_active,
        source_grid_id=cam.source_grid_id,
        codec=cam.codec,
        stream_width=cam.stream_width,
        stream_height=cam.stream_height,
        stream_fps=cam.stream_fps,
        bitrate_kbps=cam.bitrate_kbps,
        rtsp_url=cam.rtsp_url,
        whep_url=cam.whep_url,
        hls_url=cam.hls_url,
        created_at=cam.created_at,
        updated_at=cam.updated_at,
    )


def _set_location(cam: CameraModel, lat: Optional[float], lon: Optional[float]):
    """Set the PostGIS geography column from lat/lon floats."""
    if lat is not None and lon is not None:
        cam.location = func.ST_GeogFromText(
            f"SRID=4326;POINT({lon} {lat})"
        )
    elif lat is None and lon is None:
        pass  # leave unchanged
    else:
        raise HTTPException(
            status_code=422,
            detail="Both latitude and longitude must be provided together.",
        )


# ── endpoints ───────────────────────────────────────────────────


@router.get("", response_model=list[CameraSchema])
def list_cameras(
    department_id: Optional[uuid.UUID] = Query(None),
    district_id: Optional[uuid.UUID] = Query(None),
    connectivity_status: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=1000, description="Max records to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    q = db.query(CameraModel).options(
        joinedload(CameraModel.department),
        joinedload(CameraModel.district),
    )
    if department_id:
        q = q.filter(CameraModel.department_id == department_id)
    if district_id:
        q = q.filter(CameraModel.district_id == district_id)
    if connectivity_status:
        q = q.filter(CameraModel.connectivity_status == connectivity_status)
    if is_active is not None:
        q = q.filter(CameraModel.is_active == is_active)
    else:
        # Default: only active cameras
        q = q.filter(CameraModel.is_active == True)  # noqa: E712

    cameras = q.order_by(CameraModel.name).limit(limit).offset(offset).all()
    return [_camera_to_schema(c) for c in cameras]


@router.post("", response_model=CameraSchema, status_code=201)
def create_camera(
    body: CameraCreate,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin")),
):
    department_id = body.department_id
    if current_user.department_id:
        if department_id and department_id != current_user.department_id:
            raise HTTPException(
                status_code=403,
                detail="Department administrators can only create cameras in their assigned department.",
            )
        # Left blank on the form -> default to the admin's own department,
        # rather than silently creating a camera with no department that
        # no dept_admin (including this one) could later manage.
        department_id = department_id or current_user.department_id

    cam = CameraModel(
        name=body.name,
        department_id=department_id,
        district_id=body.district_id,
        camera_type=body.camera_type,
        ownership=body.ownership,
        storage_type=body.storage_type,
        retention_days=body.retention_days,
        vms_url=body.vms_url,
        connectivity_status=body.connectivity_status,
    )
    _set_location(cam, body.latitude, body.longitude)
    db.add(cam)
    db.execute(text("SET LOCAL app.current_user_id = :uid"), {"uid": str(current_user.id)})
    db.commit()
    db.refresh(cam)
    # Eagerly load relationships for the response
    _ = cam.department
    _ = cam.district
    return _camera_to_schema(cam)


@router.post("/bulk", response_model=BulkImportResult)
async def bulk_import(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin")),
):
    """
    CSV bulk import — parse rows, validate, insert.
    Returns count of created/skipped/errored.  No wizard, no preview step.
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    # Size limit (5MB) to prevent memory exhaustion
    if file.size and file.size > 5 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 5MB).")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 5MB).")

    text_content = content.decode("utf-8-sig")  # handle BOM
    reader = csv.DictReader(io.StringIO(text_content))

    created = 0
    skipped = 0
    errored = 0
    errors: list[str] = []

    db.execute(text("SET LOCAL app.current_user_id = :uid"), {"uid": str(current_user.id)})

    for i, row in enumerate(reader, start=2):  # row 1 = header
        try:
            name = row.get("name", "").strip()
            if not name:
                errors.append(f"Row {i}: missing 'name'")
                errored += 1
                continue

            cam = CameraModel(
                name=name,
                camera_type=row.get("camera_type", "").strip() or None,
                ownership=row.get("ownership", "").strip() or None,
                storage_type=row.get("storage_type", "").strip() or None,
                retention_days=(
                    int(row["retention_days"])
                    if row.get("retention_days", "").strip()
                    else None
                ),
                vms_url=row.get("vms_url", "").strip() or None,
                connectivity_status=(
                    row.get("connectivity_status", "").strip() or "offline"
                ),
            )

            # Department by name lookup
            dept_name = row.get("department", "").strip()
            if dept_name:
                from shared.db.models import Department as DeptModel

                dept = (
                    db.query(DeptModel).filter(DeptModel.name == dept_name).first()
                )
                if dept:
                    cam.department_id = dept.id

            # Enforce department scoping for dept_admin during bulk import
            if current_user.department_id and cam.department_id and cam.department_id != current_user.department_id:
                errors.append(f"Row {i}: cannot import camera for another department")
                errored += 1
                continue

            # District by name lookup
            dist_name = row.get("district", "").strip()
            if dist_name:
                from shared.db.models import District as DistModel

                dist = (
                    db.query(DistModel).filter(DistModel.name == dist_name).first()
                )
                if dist:
                    cam.district_id = dist.id

            # Location
            lat_str = row.get("latitude", "").strip()
            lon_str = row.get("longitude", "").strip()
            if lat_str and lon_str:
                lat = float(lat_str)
                lon = float(lon_str)
                cam.location = func.ST_GeogFromText(
                    f"SRID=4326;POINT({lon} {lat})"
                )

            with db.begin_nested():
                db.add(cam)
                db.flush()
            created += 1
        except Exception as exc:
            errors.append(f"Row {i}: {exc}")
            errored += 1

    if created > 0:
        db.commit()

    return BulkImportResult(
        created=created, skipped=skipped, errored=errored, errors=errors
    )


@router.get("/{camera_id}", response_model=CameraSchema)
def get_camera(
    camera_id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    cam = (
        db.query(CameraModel)
        .options(
            joinedload(CameraModel.department),
            joinedload(CameraModel.district),
        )
        .filter(CameraModel.id == camera_id)
        .first()
    )
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")
    return _camera_to_schema(cam)


@router.patch("/{camera_id}", response_model=CameraSchema)
def update_camera(
    camera_id: uuid.UUID,
    body: CameraUpdate,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin")),
):
    cam = db.query(CameraModel).filter(CameraModel.id == camera_id).first()
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")

    if current_user.department_id and cam.department_id != current_user.department_id:
        raise HTTPException(
            status_code=403,
            detail="Department administrators can only manage cameras in their assigned department.",
        )

    update_data = body.model_dump(exclude_unset=True)
    lat = update_data.pop("latitude", None)
    lon = update_data.pop("longitude", None)

    if "department_id" in update_data and current_user.department_id:
        if update_data["department_id"] != current_user.department_id:
            raise HTTPException(
                status_code=403,
                detail="Cannot transfer camera to a different department.",
            )

    for field, value in update_data.items():
        setattr(cam, field, value)

    _set_location(cam, lat, lon)

    db.execute(text("SET LOCAL app.current_user_id = :uid"), {"uid": str(current_user.id)})
    db.commit()
    db.refresh(cam)
    # Eagerly load relationships
    _ = cam.department
    _ = cam.district
    return _camera_to_schema(cam)


@router.delete("/{camera_id}", response_model=CameraSchema)
def delete_camera(
    camera_id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin")),
):
    """
    Soft delete — sets is_active = false.
    The DB trigger automatically timestamps decommissioned_at and
    writes a status_history row.
    """
    cam = db.query(CameraModel).filter(CameraModel.id == camera_id).first()
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")
    if not cam.is_active:
        raise HTTPException(status_code=400, detail="Camera is already deactivated")

    if current_user.department_id and cam.department_id != current_user.department_id:
        raise HTTPException(
            status_code=403,
            detail="Department administrators can only manage cameras in their assigned department.",
        )

    cam.is_active = False
    db.execute(text("SET LOCAL app.current_user_id = :uid"), {"uid": str(current_user.id)})
    db.commit()
    db.refresh(cam)
    _ = cam.department
    _ = cam.district
    return _camera_to_schema(cam)


@router.get("/{camera_id}/history")
def get_camera_history(
    camera_id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Audit trail for one camera — status_history rows."""
    cam = db.query(CameraModel).filter(CameraModel.id == camera_id).first()
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")

    rows = (
        db.query(StatusHistoryModel)
        .filter(StatusHistoryModel.camera_id == camera_id)
        .order_by(StatusHistoryModel.changed_at.desc())
        .all()
    )
    return [
        {
            "id": str(r.id),
            "camera_id": str(r.camera_id),
            "changed_field": r.changed_field,
            "old_value": r.old_value,
            "new_value": r.new_value,
            "changed_by": str(r.changed_by) if r.changed_by else None,
            "changed_at": r.changed_at.isoformat(),
        }
        for r in rows
    ]
