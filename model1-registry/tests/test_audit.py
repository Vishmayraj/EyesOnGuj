"""
Tests for routers/audit.py (AuditReport1.md #8: "Global audit log isn't
role-scoped").

Before this fix, GET /api/v1/audit only required *some* logged-in user
(Depends(get_current_user)) - a viewer1 account (no department, read-only
per Project_Context.md §3) could pull the complete cross-department
camera change history. This file locks in the resolved scoping:

  * viewer -> 403 (not authorized for this endpoint at all)
  * dept_admin / operator with a department_id -> only that department's
    history
  * dept_admin / operator with no department_id (global) -> full history,
    same as before the fix
"""

import pytest


def test_audit_log_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/audit")
    assert resp.status_code == 401


def test_viewer_cannot_access_audit_log(viewer_client):
    resp = viewer_client.get("/api/v1/audit")
    assert resp.status_code == 403


def test_operator_can_access_audit_log(operator_client):
    """operator1 has no department (see seed.sql) - global visibility."""
    resp = operator_client.get("/api/v1/audit")
    assert resp.status_code == 200


def test_dept_admin_audit_log_scoped_to_own_department(
    admin_home_client, admin_rto_client, home_camera, rto_camera
):
    """A department-scoped dept_admin only sees status_history rows for
    cameras in their own department - not the other department's."""
    # Generate one status_history row per camera via the real API (the
    # DB trigger writes status_history on connectivity_status change).
    admin_home_client.patch(
        f"/api/v1/cameras/{home_camera['id']}",
        json={"connectivity_status": "offline"},
    )
    admin_rto_client.patch(
        f"/api/v1/cameras/{rto_camera['id']}",
        json={"connectivity_status": "offline"},
    )

    home_admin_rows = admin_home_client.get("/api/v1/audit").json()
    assert any(r["camera_id"] == home_camera["id"] for r in home_admin_rows)
    assert not any(r["camera_id"] == rto_camera["id"] for r in home_admin_rows)

    rto_admin_rows = admin_rto_client.get("/api/v1/audit").json()
    assert any(r["camera_id"] == rto_camera["id"] for r in rto_admin_rows)
    assert not any(r["camera_id"] == home_camera["id"] for r in rto_admin_rows)


@pytest.fixture()
def home_camera(admin_home_client, home_dept_id):
    resp = admin_home_client.get(
        "/api/v1/cameras", params={"department_id": home_dept_id}
    )
    cams = resp.json()
    assert cams, "seed.sql should have at least one camera in Home Department"
    return cams[0]


@pytest.fixture()
def rto_camera(admin_rto_client, rto_dept_id):
    resp = admin_rto_client.get(
        "/api/v1/cameras", params={"department_id": rto_dept_id}
    )
    cams = resp.json()
    assert cams, "seed.sql should have at least one camera in RTO"
    return cams[0]


@pytest.fixture()
def home_dept_id(admin_home_client):
    depts = admin_home_client.get("/api/v1/departments").json()
    for d in depts:
        if "home department" in d["name"].lower():
            return d["id"]
    raise LookupError("no seeded department matching 'Home Department'")


@pytest.fixture()
def rto_dept_id(admin_home_client):
    depts = admin_home_client.get("/api/v1/departments").json()
    for d in depts:
        if "rto" in d["name"].lower():
            return d["id"]
    raise LookupError("no seeded department matching 'RTO'")
