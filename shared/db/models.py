"""
SQLAlchemy ORM models — mirrors shared/db/schema.sql column-for-column.

Do NOT add columns here that aren't in schema.sql.  The DB is the source
of truth; this file describes it, it doesn't extend it.
"""

import uuid
from datetime import datetime

from geoalchemy2 import Geography
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    REAL,
    String,
    Text,
    CheckConstraint,
    text,
)
from sqlalchemy.types import UserDefinedType
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class PGVector(UserDefinedType):
    """Custom SQLAlchemy type mapping to pgvector VECTOR(n)."""
    def __init__(self, dim=512):
        self.dim = dim

    def get_col_spec(self, **kw):
        return f"VECTOR({self.dim})"

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, (list, tuple)):
                return "[" + ",".join(str(float(x)) for x in value) + "]"
            return value
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, str):
                cleaned = value.strip("[]() ")
                if not cleaned:
                    return []
                return [float(x) for x in cleaned.split(",")]
            return value
        return process


class Base(DeclarativeBase):
    pass


# ── Shared foundation ──────────────────────────────────────────


class Department(Base):
    __tablename__ = "departments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False, unique=True)
    category = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    cameras = relationship("Camera", back_populates="department")
    users = relationship("User", back_populates="department")


class District(Base):
    __tablename__ = "districts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False, unique=True)
    boundary = Column(Geography("MULTIPOLYGON", srid=4326))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    cameras = relationship("Camera", back_populates="district")

    __table_args__ = (
        Index("idx_districts_boundary", "boundary", postgresql_using="gist"),
    )


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(Text, nullable=False, unique=True)
    email = Column(Text, unique=True)
    hashed_password = Column(Text, nullable=False)
    role = Column(
        Text,
        nullable=False,
        info={"check": "role IN ('dept_admin', 'operator', 'viewer')"},
    )
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="SET NULL")
    )
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    department = relationship("Department", back_populates="users")

    __table_args__ = (
        CheckConstraint(
            "role IN ('dept_admin', 'operator', 'viewer')", name="users_role_check"
        ),
        Index("idx_users_department", "department_id"),
    )


class VMSSystem(Base):
    __tablename__ = "vms_systems"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    vendor = Column(Text)
    protocol = Column(Text)
    ownership = Column(
        Text,
        nullable=False,
        default="government",
        info={"check": "ownership IN ('government', 'private')"},
    )
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="SET NULL")
    )
    status = Column(
        Text,
        nullable=False,
        default="unknown",
        info={"check": "status IN ('connected', 'disconnected', 'unknown')"},
    )
    camera_count = Column(Integer, nullable=False, default=0)
    last_heartbeat = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    department = relationship("Department")
    cameras = relationship("Camera", back_populates="vms_system")

    __table_args__ = (
        CheckConstraint(
            "ownership IN ('government', 'private')", name="vms_systems_ownership_check"
        ),
        CheckConstraint(
            "status IN ('connected', 'disconnected', 'unknown')", name="vms_systems_status_check"
        ),
        Index("idx_vms_systems_department", "department_id"),
    )


# ── Model 1 — Registry & GIS ───────────────────────────────────


class Camera(Base):
    __tablename__ = "cameras"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)

    # Registry / onboarding fields
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="RESTRICT")
    )
    district_id = Column(
        UUID(as_uuid=True), ForeignKey("districts.id", ondelete="SET NULL")
    )
    location = Column(Geography("POINT", srid=4326))
    camera_type = Column(Text)
    ownership = Column(Text)
    storage_type = Column(Text)
    retention_days = Column(Integer)
    vms_url = Column(Text)
    connectivity_status = Column(
        Text, nullable=False, server_default="offline"
    )
    is_active = Column(Boolean, nullable=False, default=True)
    decommissioned_at = Column(DateTime(timezone=True))

    # Grid catalogue fields (mirrored from GET /api/ingest)
    source_grid_id = Column(Text)
    location_label = Column(Text)
    is_live = Column(Boolean)
    codec = Column(Text)
    stream_width = Column(Integer)
    stream_height = Column(Integer)
    stream_fps = Column(REAL)
    bitrate_kbps = Column(Integer)
    rtsp_url = Column(Text)
    whep_url = Column(Text)
    hls_url = Column(Text)
    grid_synced_at = Column(DateTime(timezone=True))

    # VMS federation fields
    vms_system_id = Column(
        UUID(as_uuid=True), ForeignKey("vms_systems.id", ondelete="SET NULL")
    )

    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    department = relationship("Department", back_populates="cameras")
    district = relationship("District", back_populates="cameras")
    status_history = relationship("StatusHistory", back_populates="camera")
    vms_system = relationship("VMSSystem", back_populates="cameras")

    __table_args__ = (
        CheckConstraint(
            "connectivity_status IN ('online', 'offline', 'maintenance')",
            name="cameras_connectivity_status_check",
        ),
        Index("idx_cameras_location", "location", postgresql_using="gist"),
        Index("idx_cameras_department", "department_id"),
        Index("idx_cameras_district", "district_id"),
        Index("idx_cameras_status", "connectivity_status"),
        Index("idx_cameras_active", "is_active"),
        Index("idx_cameras_vms_system", "vms_system_id"),
        Index(
            "idx_cameras_source_per_system",
            "vms_system_id",
            "source_grid_id",
            unique=True,
        ),
    )


class StatusHistory(Base):
    __tablename__ = "status_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    camera_id = Column(
        UUID(as_uuid=True),
        ForeignKey("cameras.id", ondelete="RESTRICT"),
        nullable=False,
    )
    changed_field = Column(Text, nullable=False)
    old_value = Column(Text)
    new_value = Column(Text)
    changed_by = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    changed_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    camera = relationship("Camera", back_populates="status_history")

    __table_args__ = (
        Index("idx_status_history_camera", "camera_id", text("changed_at DESC")),
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    action = Column(Text, nullable=False)
    resource_type = Column(Text, nullable=False)
    resource_id = Column(Text, nullable=True)
    details = Column(JSONB, nullable=True)
    user_id = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="SET NULL")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    user = relationship("User")
    department = relationship("Department")

    __table_args__ = (
        Index("idx_audit_logs_created_at", text("created_at DESC")),
        Index("idx_audit_logs_department", "department_id"),
    )

# ── Model 2 — Unified Viewer & Analytics ───────────────────────


class VehicleWatchlist(Base):
    __tablename__ = "vehicles_watchlist"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plate_number = Column(Text, nullable=False)
    category = Column(
        Text,
        nullable=False,
        info={"check": "category IN ('stolen', 'wanted', 'blacklisted')"},
    )
    reported_date = Column(Date)
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="SET NULL")
    )
    description = Column(Text)
    status = Column(
        Text,
        nullable=False,
        default="active",
        info={"check": "status IN ('active', 'resolved')"},
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    department = relationship("Department")

    __table_args__ = (
        CheckConstraint(
            "category IN ('stolen', 'wanted', 'blacklisted')",
            name="vehicles_watchlist_category_check",
        ),
        CheckConstraint(
            "status IN ('active', 'resolved')",
            name="vehicles_watchlist_status_check",
        ),
        Index("idx_vehicles_watchlist_plate", "plate_number"),
    )


class PersonWatchlist(Base):
    __tablename__ = "persons_watchlist"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    category = Column(
        Text,
        nullable=False,
        info={"check": "category IN ('wanted', 'missing', 'suspect')"},
    )
    face_embedding = Column(PGVector(512))
    photo_path = Column(Text)
    status = Column(
        Text,
        nullable=False,
        default="active",
        info={"check": "status IN ('active', 'resolved')"},
    )
    department_id = Column(
        UUID(as_uuid=True), ForeignKey("departments.id", ondelete="SET NULL")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    department = relationship("Department")

    __table_args__ = (
        CheckConstraint(
            "category IN ('wanted', 'missing', 'suspect')",
            name="persons_watchlist_category_check",
        ),
        CheckConstraint(
            "status IN ('active', 'resolved')",
            name="persons_watchlist_status_check",
        ),
    )


class VehicleTrack(Base):
    __tablename__ = "vehicle_tracks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plate_number = Column(Text)
    vehicle_color = Column(Text)
    vehicle_type = Column(Text)
    first_seen = Column(DateTime(timezone=True), nullable=False)
    last_seen = Column(DateTime(timezone=True), nullable=False)
    is_watchlisted = Column(Boolean, nullable=False, default=False)

    detections = relationship("Detection", back_populates="vehicle_track")

    __table_args__ = (
        Index("idx_vehicle_tracks_plate", "plate_number"),
    )


class Detection(Base):
    __tablename__ = "detections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    camera_id = Column(
        UUID(as_uuid=True), ForeignKey("cameras.id", ondelete="RESTRICT"), nullable=False
    )
    timestamp = Column(DateTime(timezone=True), nullable=False)
    event_type = Column(Text, nullable=False, default="vehicle_detection")
    detected_plate = Column(Text)
    vehicle_type = Column(Text)
    confidence = Column(REAL)
    cropped_image_path = Column(Text)
    raw_payload = Column(JSONB)
    source_timestamp = Column(DateTime(timezone=True))
    vehicle_track_id = Column(
        UUID(as_uuid=True), ForeignKey("vehicle_tracks.id", ondelete="SET NULL")
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")

    camera = relationship("Camera")
    vehicle_track = relationship("VehicleTrack", back_populates="detections")
    alerts = relationship("Alert", back_populates="detection")

    __table_args__ = (
        Index("idx_detections_camera_time", "camera_id", text('"timestamp" DESC')),
        Index("idx_detections_track_time", "vehicle_track_id", "timestamp"),
        Index("idx_detections_plate", "detected_plate"),
    )


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detection_id = Column(
        UUID(as_uuid=True), ForeignKey("detections.id", ondelete="CASCADE"), nullable=False
    )
    watchlist_id = Column(
        UUID(as_uuid=True), ForeignKey("vehicles_watchlist.id", ondelete="RESTRICT"), nullable=False
    )
    alert_type = Column(Text, nullable=False, default="vehicle_match")
    severity = Column(
        Text,
        info={"check": "severity IN ('low', 'medium', 'high', 'critical')"},
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")
    acknowledged_by = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    acknowledged_at = Column(DateTime(timezone=True))

    detection = relationship("Detection", back_populates="alerts")
    watchlist = relationship("VehicleWatchlist")
    acknowledged_by_user = relationship("User")

    __table_args__ = (
        CheckConstraint(
            "severity IN ('low', 'medium', 'high', 'critical')",
            name="alerts_severity_check",
        ),
        Index("idx_alerts_watchlist", "watchlist_id"),
        Index("idx_alerts_created", text("created_at DESC")),
    )


class PersonAlert(Base):
    __tablename__ = "person_alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    person_id = Column(
        UUID(as_uuid=True), ForeignKey("persons_watchlist.id", ondelete="CASCADE"), nullable=False
    )
    camera_id = Column(
        UUID(as_uuid=True), ForeignKey("cameras.id", ondelete="SET NULL"), nullable=True, default=None
    )
    similarity_score = Column(REAL, nullable=False)
    distance = Column(REAL, nullable=False)
    face_crop_path = Column(Text)
    frame_timestamp = Column(DateTime(timezone=True), nullable=False, server_default="now()")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default="now()")
    acknowledged_by = Column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    acknowledged_at = Column(DateTime(timezone=True))

    person = relationship("PersonWatchlist", backref="person_alerts")
    acknowledged_by_user = relationship("User")

    __table_args__ = (
        Index("idx_person_alerts_person", "person_id"),
        Index("idx_person_alerts_created", text("created_at DESC")),
    )

