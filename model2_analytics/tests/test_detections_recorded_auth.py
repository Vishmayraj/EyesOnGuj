"""
Anonymous-401 / role-403 regression tests for detections.py and
recorded.py (AuditReport2.md findings 2, 3, 4 -- both routers shipped
with zero auth on any endpoint; fixed in commit 2413340, this locks
that fix in so it can't silently regress).

Follows the exact pattern model1-registry/tests/test_streams.py already
uses for its own AuditReport1.md finding 1.1 fix: anonymous requests
must get 401, and the two start/stop-style control endpoints (which can
turn a live detection/recording session on or off for anyone) must
additionally reject a logged-in `viewer` with 403, since only
dept_admin/operator should be allowed to call them.

Needs the same real Postgres `sentinel_test` setup as
model1-registry/tests (see this directory's conftest.py docstring).
The websocket routes (`/ws/detections`, `/ws/recorded/{job_id}`) are not
covered here -- TestClient's websocket_connect() reports an unauthorized
close differently than resp.status_code does, and is worth its own
focused test rather than being forced into this file's shape.
"""

import uuid


# ── detections.py ───────────────────────────────────────────────────

def test_detection_events_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/detection/events")
    assert resp.status_code == 401


def test_detection_history_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/detections")
    assert resp.status_code == 401


def test_detection_stats_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/detections/stats")
    assert resp.status_code == 401


def test_start_detection_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/detections/start")
    assert resp.status_code == 401


def test_stop_detection_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/detections/stop")
    assert resp.status_code == 401


def test_start_detection_rejects_viewer(viewer_client):
    """start/stop are operational controls, not read access -- viewer
    should be excluded the same way watchlist.py/persons_watchlist.py
    exclude viewer from write/control actions."""
    resp = viewer_client.post("/api/v1/detections/start")
    assert resp.status_code == 403


def test_stop_detection_rejects_viewer(viewer_client):
    resp = viewer_client.post("/api/v1/detections/stop")
    assert resp.status_code == 403


def test_detection_history_accessible_to_any_logged_in_role(viewer_client):
    resp = viewer_client.get("/api/v1/detections")
    assert resp.status_code == 200


# ── recorded.py ──────────────────────────────────────────────────────

def test_recorded_cameras_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/recorded/cameras")
    assert resp.status_code == 401


def test_recorded_upload_requires_auth(anon_client):
    resp = anon_client.post(
        "/api/v1/recorded/upload",
        files={"file": ("clip.mp4", b"not-a-real-video", "video/mp4")},
    )
    assert resp.status_code == 401


def test_recorded_upload_rejects_viewer(viewer_client):
    resp = viewer_client.post(
        "/api/v1/recorded/upload",
        files={"file": ("clip.mp4", b"not-a-real-video", "video/mp4")},
    )
    assert resp.status_code == 403


def test_recorded_start_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/recorded/start", json={"job_id": str(uuid.uuid4())})
    assert resp.status_code == 401


def test_recorded_pause_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/recorded/pause", json={"job_id": str(uuid.uuid4())})
    assert resp.status_code == 401


def test_recorded_resume_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/recorded/resume", json={"job_id": str(uuid.uuid4())})
    assert resp.status_code == 401


def test_recorded_stop_requires_auth(anon_client):
    resp = anon_client.post("/api/v1/recorded/stop", json={"job_id": str(uuid.uuid4())})
    assert resp.status_code == 401


def test_recorded_start_rejects_viewer(viewer_client):
    resp = viewer_client.post("/api/v1/recorded/start", json={"job_id": str(uuid.uuid4())})
    assert resp.status_code == 403


def test_recorded_status_requires_auth(anon_client):
    resp = anon_client.get(f"/api/v1/recorded/status/{uuid.uuid4()}")
    assert resp.status_code == 401


def test_recorded_status_inaccessible_to_viewer_role(viewer_client):
    # A viewer should not be able to access the status endpoint
    resp = viewer_client.get(f"/api/v1/recorded/status/{uuid.uuid4()}")
    assert resp.status_code == 403
