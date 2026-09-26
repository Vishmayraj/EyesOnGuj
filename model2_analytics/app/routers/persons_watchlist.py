"""
Model 2 — Person Watchlist API Router
Endpoints:
  - POST   /api/v1/watchlist/persons       (Register person with photo, quality check, and 512-d embedding)
  - GET    /api/v1/watchlist/persons       (List / Filter person watchlist entries)
  - GET    /api/v1/watchlist/persons/{id}  (Get single person watchlist record)
  - PATCH  /api/v1/watchlist/persons/{id}  (Update person record or status)
  - DELETE /api/v1/watchlist/persons/{id}  (Delete person watchlist entry)
"""

import io
import uuid
import logging
from pathlib import Path
from typing import Optional, List

import cv2
import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.auth.dependencies import require_role
from shared.db.models import User as UserModel, PersonWatchlist as PersonWatchlistModel
from shared.db.session import get_db
from shared.schemas.persons_watchlist import (
    PersonWatchlistResponse,
    PersonWatchlistUpdate,
    FaceQualityMetrics,
)
from pipeline.faceembedding.quality_checker import FaceQualityChecker
from pipeline.faceembedding.encoder import FaceEmbeddingEngine

logger = logging.getLogger("sentinel.persons.router")

router = APIRouter(prefix="/api/v1/watchlist/persons", tags=["person-watchlist"])

# Directory to persist uploaded/cropped face portraits
FACES_DIR = Path("/app/model2_analytics/uploads/persons")
if not FACES_DIR.exists():
    FACES_DIR = Path(__file__).resolve().parents[2] / "uploads" / "persons"
FACES_DIR.mkdir(parents=True, exist_ok=True)

MAX_PHOTO_SIZE = 10 * 1024 * 1024  # 10 MB limit for portrait photos
ALLOWED_PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png"}

# Lazy singletons for heavy models
_quality_checker: Optional[FaceQualityChecker] = None
_embedding_engine: Optional[FaceEmbeddingEngine] = None


def get_quality_checker() -> FaceQualityChecker:
    global _quality_checker
    if _quality_checker is None:
        _quality_checker = FaceQualityChecker()
    return _quality_checker


def get_embedding_engine() -> FaceEmbeddingEngine:
    global _embedding_engine
    if _embedding_engine is None:
        _embedding_engine = FaceEmbeddingEngine()
    return _embedding_engine


def _format_person_response(
    item: PersonWatchlistModel,
    quality_metrics: Optional[FaceQualityMetrics] = None,
) -> dict:
    has_emb = item.face_embedding is not None
    emb_dim = len(item.face_embedding) if has_emb and isinstance(item.face_embedding, (list, tuple)) else (512 if has_emb else None)
    return {
        "id": item.id,
        "name": item.name,
        "category": item.category,
        "status": item.status,
        "photo_path": item.photo_path,
        "has_embedding": has_emb,
        "embedding_dim": emb_dim,
        "created_at": item.created_at,
        "quality_metrics": quality_metrics,
    }


@router.post("", response_model=PersonWatchlistResponse, status_code=status.HTTP_201_CREATED)
async def create_watchlist_person(
    name: str = Form(..., min_length=2, max_length=120, description="Full name or alias of the person"),
    category: str = Form(..., description="Watchlist category: wanted, missing, or suspect"),
    status_val: str = Form("active", alias="status", description="Status: active or resolved"),
    photo: UploadFile = File(..., description="Reference portrait photo (JPEG or PNG)"),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Register a new individual into the Person Watchlist.
    
    Pipeline:
      1. Validates input payload and category constraints.
      2. Runs 5-gate FaceQualityChecker (integrity, count, boundary anti-clipping, sharpness, 3D pose).
      3. If any quality gate fails, returns 422 Unprocessable Entity with exact diagnostic reason.
      4. If quality passes, aligns face and generates a unit-normalized 512-d InceptionResnetV1 embedding.
      5. Saves the cropped reference portrait to disk.
      6. Persists record and vector embedding into PostgreSQL (pgvector).
    """
    clean_name = name.strip()
    if len(clean_name) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Person name must be at least 2 characters long.",
        )

    clean_category = category.strip().lower()
    if clean_category not in ("wanted", "missing", "suspect"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid category '{category}'. Allowed categories: 'wanted', 'missing', 'suspect'.",
        )

    clean_status = status_val.strip().lower()
    if clean_status not in ("active", "resolved"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid status '{status_val}'. Allowed status: 'active', 'resolved'.",
        )

    # Validate extension and read uploaded photo bytes with size enforcement
    filename = photo.filename or "portrait.jpg"
    import re
    clean_filename = re.sub(r"[^\w\-.]", "_", filename)
    ext = Path(clean_filename).suffix.lower()
    if ext not in ALLOWED_PHOTO_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported image format '{ext}'. Allowed: {', '.join(sorted(ALLOWED_PHOTO_EXTENSIONS))}",
        )

    photo_buffer = bytearray()
    try:
        while True:
            chunk = await photo.read(1024 * 1024)
            if not chunk:
                break
            photo_buffer.extend(chunk)
            if len(photo_buffer) > MAX_PHOTO_SIZE:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="Photo file exceeds maximum permitted size of 10 MB",
                )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read uploaded photo file: {str(e)}",
        )

    photo_bytes = bytes(photo_buffer)
    if not photo_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded photo file is empty.",
        )

    # ── Step 1: Quality Gatekeeper Check ────────────────────────
    checker = get_quality_checker()
    quality_res = checker.evaluate(photo_bytes)

    if not quality_res.passed:
        logger.warning(f"Watchlist registration rejected for '{clean_name}': {quality_res.rejection_reason}")
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "error": "FaceQualityCheckFailed",
                "message": quality_res.rejection_reason,
                "metrics": quality_res.metrics,
            },
        )

    # ── Step 2: 512-d Face Embedding Extraction ─────────────────
    encoder = get_embedding_engine()
    # CRITICAL: Pass the full original oriented image (matching the landmark coordinate space).
    # Passing face_crop here would mismatch landmark coordinates and corrupt alignment.
    full_img = quality_res.full_image
    if full_img is None:
        np_buf = np.frombuffer(photo_bytes, dtype=np.uint8)
        full_img = cv2.imdecode(np_buf, cv2.IMREAD_COLOR)
        if full_img is None:
            raise HTTPException(status_code=400, detail="Invalid image file format")
        h, w = full_img.shape[:2]
        if h > 4096 or w > 4096:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Image dimensions exceed maximum (4096x4096)",
            )

    try:
        embedding = encoder.extract_embedding(full_img, landmarks=quality_res.landmarks)
    except Exception as e:
        logger.error(f"Face embedding extraction failed for '{clean_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Face embedding generation failed: {str(e)}",
        )

    # ── Step 3: Save Reference Face Portrait to Disk ─────────────
    person_id = uuid.uuid4()
    photo_filename = f"{person_id}.jpg"
    photo_dest = FACES_DIR / photo_filename
    
    # Save the cropped face portrait to disk for visual reference in UI
    save_img = quality_res.face_crop if quality_res.face_crop is not None else full_img
    cv2.imwrite(str(photo_dest), save_img)
    rel_photo_path = f"/api/v1/watchlist/persons/photos/{photo_filename}"

    # ── Step 4: Persist in PostgreSQL (pgvector) ─────────────────
    new_entry = PersonWatchlistModel(
        id=person_id,
        name=clean_name,
        category=clean_category,
        face_embedding=embedding,
        photo_path=rel_photo_path,
        status=clean_status,
        department_id=current_user.department_id,
    )
    db.add(new_entry)
    db.commit()
    db.refresh(new_entry)

    logger.info(f"Person watchlist registered: '{clean_name}' (ID: {person_id}, Category: {clean_category})")

    # Construct quality metrics response
    quality_metrics = FaceQualityMetrics(
        face_detected=True,
        sharpness_score=quality_res.sharpness_face,
        face_resolution=[quality_res.face_bbox[2], quality_res.face_bbox[3]] if quality_res.face_bbox else [0, 0],
        yaw_ratio=round(quality_res.yaw_deg, 2),
        roll_angle_deg=round(quality_res.roll_deg, 1),
        brightness_mean=round(quality_res.brightness_mean, 1),
        is_frontal=True,
        quality_passed=True,
        rejection_reason=None,
    )

    return _format_person_response(new_entry, quality_metrics=quality_metrics)


@router.get("", response_model=List[PersonWatchlistResponse])
def list_watchlist_persons(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (active/resolved)"),
    category: Optional[str] = Query(None, description="Filter by category (wanted/missing/suspect)"),
    name: Optional[str] = Query(None, description="Search by person name"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Retrieve all person watchlist entries with optional filtering.
    """
    query = db.query(PersonWatchlistModel)

    if status_filter:
        query = query.filter(PersonWatchlistModel.status == status_filter.strip().lower())
    if category:
        query = query.filter(PersonWatchlistModel.category == category.strip().lower())
    if name:
        query = query.filter(PersonWatchlistModel.name.ilike(f"%{name.strip()}%"))
    if current_user.department_id:
        query = query.filter(PersonWatchlistModel.department_id == current_user.department_id)

    items = query.order_by(desc(PersonWatchlistModel.created_at)).offset(offset).limit(limit).all()
    return [_format_person_response(item) for item in items]


@router.get("/{id}", response_model=PersonWatchlistResponse)
def get_watchlist_person(
    id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Get details of a specific person watchlist entry by UUID.
    """
    query = db.query(PersonWatchlistModel).filter(PersonWatchlistModel.id == id)
    if current_user.department_id:
        query = query.filter(PersonWatchlistModel.department_id == current_user.department_id)
        
    item = query.first()
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person watchlist entry with ID '{id}' not found.",
        )
    return _format_person_response(item)


@router.patch("/{id}", response_model=PersonWatchlistResponse)
def update_watchlist_person(
    id: uuid.UUID,
    payload: PersonWatchlistUpdate,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Update person details or status (e.g., mark as 'resolved' when apprehended or located).
    """
    query = db.query(PersonWatchlistModel).filter(PersonWatchlistModel.id == id)
    if current_user.department_id:
        query = query.filter(PersonWatchlistModel.department_id == current_user.department_id)
        
    item = query.first()
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person watchlist entry with ID '{id}' not found.",
        )

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == "name" and value is not None:
            clean = value.strip()
            if len(clean) < 2:
                raise HTTPException(status_code=400, detail="Name must be at least 2 characters long.")
            setattr(item, "name", clean)
        elif field in ("category", "status") and value is not None:
            setattr(item, field, value.strip().lower())
        else:
            setattr(item, field, value)

    db.commit()
    db.refresh(item)
    return _format_person_response(item)


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_watchlist_person(
    id: uuid.UUID,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Delete a person watchlist entry permanently.
    """
    query = db.query(PersonWatchlistModel).filter(PersonWatchlistModel.id == id)
    if current_user.department_id:
        query = query.filter(PersonWatchlistModel.department_id == current_user.department_id)
        
    item = query.first()
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Person watchlist entry with ID '{id}' not found.",
        )

    # Clean up photo on disk if present
    if item.photo_path:
        photo_filename = Path(item.photo_path).name
        photo_file = FACES_DIR / photo_filename
        if photo_file.exists():
            try:
                photo_file.unlink()
            except Exception as e:
                logger.warning(f"Could not delete photo file {photo_file}: {e}")

    db.delete(item)
    db.commit()
    return None


@router.get("/photos/{photo_filename}", name="get_watchlist_photo")
async def get_watchlist_photo(
    photo_filename: str,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
):
    """
    Serve a registered person's reference face portrait, secured to authorized operators.
    """
    requested = (FACES_DIR / photo_filename).resolve()
    try:
        requested.relative_to(FACES_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    if not requested.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    return FileResponse(str(requested))

