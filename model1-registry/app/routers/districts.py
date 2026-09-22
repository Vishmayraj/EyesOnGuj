import json
from fastapi import APIRouter, Depends
from sqlalchemy import func, and_
from sqlalchemy.orm import Session

from shared.db.models import Camera as CameraModel
from shared.db.models import District as DistrictModel
from shared.db.models import User as UserModel
from shared.db.session import get_db

from app.auth.dependencies import get_current_user
from shared.db.models import User as UserModel
from shared.schemas.district import District as DistrictSchema
from app.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/v1/districts", tags=["districts"])


@router.get("", response_model=list[DistrictSchema])
def list_districts(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """List all 33 Gujarat districts with camera count and PostGIS MultiPolygon boundary GeoJSON."""
    join_conditions = [
        CameraModel.district_id == DistrictModel.id,
        CameraModel.is_active == True,
    ]
    if current_user.department_id:
        join_conditions.append(CameraModel.department_id == current_user.department_id)

    rows = (
        db.query(
            DistrictModel,
            func.count(CameraModel.id).label("camera_count"),
            func.ST_AsGeoJSON(DistrictModel.boundary).label("boundary_geojson"),
        )
        .outerjoin(CameraModel, and_(*join_conditions))
        .group_by(DistrictModel.id)
        .order_by(DistrictModel.name)
        .all()
    )
    result = []
    for dist, count, boundary_json in rows:
        boundary_dict = json.loads(boundary_json) if boundary_json else None
        result.append({
            "id": dist.id,
            "name": dist.name,
            "boundary": boundary_dict,
            "created_at": dist.created_at,
            "camera_count": count,
        })
    return result
