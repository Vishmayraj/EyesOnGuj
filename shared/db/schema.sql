-- ============================================================
-- Sentinel — Model 1 (Registry & GIS) + shared foundation schema
-- ============================================================
-- Tested against a real Postgres 16 + PostGIS 3.4 instance
-- (matching infra/docker-compose.yml, db name `sentinel`) —
-- every statement below actually ran clean, not just eyeballed.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS vector;     -- pgvector, for appearance-embedding fallback matching

-- ============================================================
-- Shared foundation
-- ============================================================

CREATE TABLE departments (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    category    TEXT,            -- e.g. Home/Police, Food & Civil Supplies, RTO, Municipal Corporation
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE districts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,          -- Gujarat's 33 districts
    boundary    GEOGRAPHY(MULTIPOLYGON, 4326), -- null until a real shapefile is sourced
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_districts_boundary ON districts USING GIST (boundary);

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username        TEXT NOT NULL UNIQUE,
    email           TEXT UNIQUE,
    hashed_password TEXT NOT NULL,
    role            TEXT NOT NULL CHECK (role IN ('dept_admin', 'operator', 'viewer')),
    department_id   UUID REFERENCES departments(id) ON DELETE SET NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_users_department ON users (department_id);

-- ============================================================
-- VMS registry — one row per distinct video-management-system
-- integration (the department that owns it, which vendor/protocol
-- it speaks, whether Sentinel operates it directly or is only a
-- consumer). A department can have more than one row here — e.g.
-- a district traffic unit running its own separate system in
-- addition to the main department-wide one — and a row need not
-- be government-owned; `ownership` covers a private vendor system
-- (e.g. a mall or society's own VMS) that Sentinel has been given
-- read access to, same as it would for a government one.
-- ============================================================

CREATE TABLE vms_systems (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    vendor          TEXT,                             -- e.g. Milestone, HikCentral, Dahua DSS
    protocol        TEXT,                             -- integration mechanism: onvif, vendor-sdk, simulated, ...
    ownership       TEXT NOT NULL DEFAULT 'government'
                    CHECK (ownership IN ('government', 'private')),
    department_id   UUID REFERENCES departments(id) ON DELETE SET NULL,
    department_hint TEXT,                              -- raw hint an adapter's ILIKE lookup used
                                                         -- to resolve department_id (registration.py's
                                                         -- _resolve_department_id); NULL for manually-
                                                         -- onboarded systems, which set department_id
                                                         -- directly with no guessing involved. Kept even
                                                         -- when the match succeeded so a later failed
                                                         -- re-resolution can still be explained; only
                                                         -- meaningful to show a user when department_id
                                                         -- IS NULL (see GET /api/v3/systems).
    status          TEXT NOT NULL DEFAULT 'unknown'
                    CHECK (status IN ('connected', 'disconnected', 'unknown')),
    adapter_type    TEXT,                              -- registry.py key (windy, rest_api, onvif, ...) for
                                                         -- config-driven onboarding; NULL means this row has
                                                         -- no live adapter behind it (see POST /api/v3/systems
                                                         -- and load_dynamic_adapters() in registration.py).
    config          JSONB,                             -- adapter-specific connection config. PLAINTEXT -- see
                                                         -- migrations/002_vms_systems_adapter_config.sql's header
                                                         -- for the security caveat before this holds real secrets.
    camera_count    INT NOT NULL DEFAULT 0,
    last_heartbeat  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_vms_systems_department ON vms_systems (department_id);

-- ============================================================
-- Model 1 — Registry & GIS
-- ============================================================
--
-- `cameras` carries three kinds of columns, kept visually grouped
-- below because they come from different places and get filled
-- in at different times:
--
--   1. Registry/onboarding fields (Project_Context.md §3) — set by
--      us: department, district, ownership, retention, RBAC-relevant
--      metadata. These are what the hackathon's Model 1 brief is
--      actually about.
--   2. Grid catalogue fields (GET /api/ingest on the camera grid
--      itself, docs/API_Contract.md §0 + model2_analytics/README.md)
--      — mirrored from the source so we don't hard-code stream URLs
--      or assume a uniform codec/resolution across ~80,000 cameras.
--      Notably: the grid gives a human-written location LABEL
--      (\"06 Timbavadi gate-Junagadh\"), not coordinates. `location`
--      is nullable in schema definition; seed.sql enriches all 30
--      cameras with coordinates and department metadata for demo completeness.
--   3. VMS federation fields — `vms_system_id` links a camera to the
--      vms_systems row that reported it. NULL means the camera came
--      from the main grid catalogue in (2) above, not from a
--      federated VMS. This is the same `cameras` table either way —
--      there is no separate camera table for federated systems.
--      `source_grid_id` is that system's own id for the camera
--      (the grid's \"id\" field for (2), or an adapter's external_id
--      for (3)); it's only unique *within* a given vms_system_id,
--      since two unrelated VMSs can both label a camera \"cam-01\".

CREATE TABLE cameras (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                  TEXT NOT NULL,

    -- registry / onboarding fields
    department_id         UUID REFERENCES departments(id) ON DELETE RESTRICT,
    district_id           UUID REFERENCES districts(id) ON DELETE SET NULL,
    location              GEOGRAPHY(POINT, 4326),          -- nullable, see note above
    camera_type           TEXT,                            -- fixed/PTZ/dome/bullet — no fixed taxonomy given yet
    ownership             TEXT,                            -- government | private (societies/malls, per the brief)
    storage_type          TEXT,                            -- cloud | local, per the brief's wording
    retention_days        INT,
    vms_url               TEXT,                            -- optional hyperlink out to native VMS viewer
    connectivity_status   TEXT NOT NULL DEFAULT 'offline'
                           CHECK (connectivity_status IN ('online', 'offline', 'maintenance')),
    is_active             BOOLEAN NOT NULL DEFAULT true,    -- soft delete — see open question on DELETE endpoint
    decommissioned_at     TIMESTAMPTZ,

    -- grid catalogue fields, mirrored from GET /api/ingest
    source_grid_id        TEXT,                            -- "id" from /api/ingest, or a federated adapter's external_id — resync key, not our PK
    location_label        TEXT,                            -- raw "location" string from /api/ingest, e.g. "06 Timbavadi gate-Junagadh"
    is_live               BOOLEAN,                          -- grid's reported "live" flag
    codec                 TEXT,                            -- h264 | hevc | '' (grid reports blank when unknown) — mixed per model2_analytics/README
    stream_width          INT,
    stream_height         INT,
    stream_fps            REAL,
    bitrate_kbps          INT,
    rtsp_url              TEXT,
    whep_url              TEXT,                            -- grid calls this "webrtc_url"; it's WHEP specifically
    hls_url               TEXT,
    grid_synced_at         TIMESTAMPTZ,                     -- last time this row was refreshed from /api/ingest

    -- VMS federation fields
    vms_system_id         UUID REFERENCES vms_systems(id) ON DELETE SET NULL,

    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_cameras_location ON cameras USING GIST (location);
CREATE INDEX idx_cameras_department ON cameras (department_id);
CREATE INDEX idx_cameras_district ON cameras (district_id);
CREATE INDEX idx_cameras_status ON cameras (connectivity_status);
CREATE INDEX idx_cameras_active ON cameras (is_active);
CREATE INDEX idx_cameras_vms_system ON cameras (vms_system_id);
CREATE UNIQUE INDEX idx_cameras_source_per_system ON cameras (vms_system_id, source_grid_id);

CREATE TABLE status_history (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    camera_id     UUID NOT NULL REFERENCES cameras(id) ON DELETE RESTRICT,
    changed_field TEXT NOT NULL,
    old_value     TEXT,
    new_value     TEXT,
    changed_by    UUID REFERENCES users(id) ON DELETE SET NULL,
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_status_history_camera ON status_history (camera_id, changed_at DESC);

-- ============================================================
-- Model 2 — Unified Viewer & Analytics
-- ============================================================

-- Watchlists
CREATE TABLE vehicles_watchlist (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plate_number  TEXT NOT NULL,             -- always store normalized (uppercase, no spaces)
    category      TEXT NOT NULL CHECK (category IN ('stolen', 'wanted', 'blacklisted')),
    reported_date DATE,
    department_id UUID REFERENCES departments(id) ON DELETE SET NULL,
    description   TEXT,
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'resolved')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX idx_vehicles_watchlist_plate_unique ON vehicles_watchlist (plate_number) WHERE status = 'active';

-- Persons watchlist with 512-d biometric face embedding
CREATE TABLE persons_watchlist (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name           TEXT NOT NULL,
    category       TEXT NOT NULL CHECK (category IN ('wanted', 'missing', 'suspect')),
    face_embedding VECTOR(512),
    photo_path     TEXT,
    status         TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'resolved')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_persons_watchlist_embedding ON persons_watchlist USING hnsw (face_embedding vector_cosine_ops);

-- Vehicle tracks — resolved global identity for vehicles
CREATE TABLE vehicle_tracks (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    plate_number          TEXT,               -- nullable: a track can exist before plate is read
    appearance_embedding  VECTOR(512),         -- representative embedding for fallback matching
    vehicle_color         TEXT,
    vehicle_type          TEXT,
    first_seen            TIMESTAMPTZ NOT NULL,
    last_seen             TIMESTAMPTZ NOT NULL,
    is_watchlisted        BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX idx_vehicle_tracks_plate ON vehicle_tracks (plate_number);
CREATE INDEX idx_vehicle_tracks_embedding ON vehicle_tracks USING hnsw (appearance_embedding vector_cosine_ops);

-- Detections — individual vehicle sightings at cameras
CREATE TABLE detections (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    camera_id           UUID NOT NULL REFERENCES cameras(id) ON DELETE RESTRICT,
    "timestamp"         TIMESTAMPTZ NOT NULL,
    event_type           TEXT NOT NULL DEFAULT 'vehicle_detection',  -- vehicle_detection | person_detection | intrusion, ...
    detected_plate       TEXT,                -- specific sighting's OCR output
    vehicle_type         TEXT,                -- car | truck | motorcycle, ... — sighting-level, separate from vehicle_tracks.vehicle_type
    confidence           REAL,
    cropped_image_path   TEXT,
    raw_payload          JSONB,               -- original vendor/adapter payload, kept for audit — nullable, only set when the source is a VMS integration that provides one
    source_timestamp     TIMESTAMPTZ,         -- when the *source* system says this happened, if it reports one distinct from "timestamp" (when we received/processed it)
    vehicle_track_id     UUID REFERENCES vehicle_tracks(id) ON DELETE SET NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_detections_camera_time ON detections (camera_id, "timestamp" DESC);
CREATE INDEX idx_detections_track_time ON detections (vehicle_track_id, "timestamp");
CREATE INDEX idx_detections_plate ON detections (detected_plate);

-- Alerts — generated on watchlist match
CREATE TABLE alerts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    detection_id     UUID NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    watchlist_id     UUID NOT NULL REFERENCES vehicles_watchlist(id) ON DELETE RESTRICT,
    alert_type       TEXT NOT NULL DEFAULT 'vehicle_match',
    severity         TEXT CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    acknowledged_at  TIMESTAMPTZ
);
CREATE INDEX idx_alerts_watchlist ON alerts (watchlist_id);
CREATE INDEX idx_alerts_created ON alerts (created_at DESC);

-- Person Alerts — generated on face watchlist match
CREATE TABLE person_alerts (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id          UUID NOT NULL REFERENCES persons_watchlist(id) ON DELETE CASCADE,
    camera_id          TEXT DEFAULT 'prerecorded',
    similarity_score   REAL NOT NULL,
    distance           REAL NOT NULL,
    face_crop_path     TEXT,
    frame_timestamp    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_by    UUID REFERENCES users(id) ON DELETE SET NULL,
    acknowledged_at    TIMESTAMPTZ
);
CREATE INDEX idx_person_alerts_person ON person_alerts (person_id);
CREATE INDEX idx_person_alerts_created ON person_alerts (created_at DESC);

-- ============================================================
-- Security & Audit Logging
-- ============================================================

CREATE TABLE audit_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action          TEXT NOT NULL,
    resource_type   TEXT,
    resource_id     TEXT,
    details         JSONB,
    user_id         UUID REFERENCES users(id) ON DELETE SET NULL,
    department_id   UUID REFERENCES departments(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_logs_user ON audit_logs (user_id);
CREATE INDEX idx_audit_logs_department ON audit_logs (department_id);
CREATE INDEX idx_audit_logs_resource ON audit_logs (resource_type, resource_id);