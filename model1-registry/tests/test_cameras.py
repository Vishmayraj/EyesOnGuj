import uuid

import pytest

from tests.conftest import unique_camera_name


def _department_id_by_name_fragment(client, fragment: str) -> str:
    """Resolve a department id via the public /api/v1/departments listing,
    using whichever client is already active — avoids ever needing a
    second login (and the cookie-jar foot-gun that comes with it; see
    conftest.py's note on independent client fixtures)."""
    depts = client.get("/api/v1/departments").json()
    for d in depts:
        if fragment.lower() in d["name"].lower():
            return d["id"]
    raise LookupError(f"no seeded department matching {fragment!r}")


@pytest.fixture()
def home_dept_id(admin_home_client):
    return _department_id_by_name_fragment(admin_home_client, "Home Department")


@pytest.fixture()
def rto_dept_id(admin_home_client):
    return _department_id_by_name_fragment(admin_home_client, "RTO")


@pytest.fixture()
def home_camera(admin_home_client, home_dept_id):
    """One seeded camera that belongs to admin_home's own department."""
    resp = admin_home_client.get(
        "/api/v1/cameras", params={"department_id": home_dept_id}
    )
    cams = resp.json()
    assert cams, "seed.sql should have at least one camera in Home Department"
    return cams[0]


@pytest.fixture()
def rto_camera(admin_rto_client, rto_dept_id):
    """One seeded camera belonging to a DIFFERENT department than admin_home."""
    resp = admin_rto_client.get(
        "/api/v1/cameras", params={"department_id": rto_dept_id}
    )
    cams = resp.json()
    assert cams, "seed.sql should have at least one camera in RTO"
    return cams[0]


# ── Read ─────────────────────────────────────────────────────────


def test_list_cameras_requires_auth(anon_client):
    resp = anon_client.get("/api/v1/cameras")
    assert resp.status_code == 401


def test_get_camera_requires_auth(anon_client):
    resp = anon_client.get(f"/api/v1/cameras/{uuid.uuid4()}")
    assert resp.status_code == 401


def test_get_camera_history_requires_auth(anon_client):
    resp = anon_client.get(f"/api/v1/cameras/{uuid.uuid4()}/history")
    assert resp.status_code == 401


def test_list_cameras_default_active_only(admin_home_client):
    resp = admin_home_client.get("/api/v1/cameras")
    assert resp.status_code == 200
    cams = resp.json()
    assert len(cams) > 0
    assert all(c["is_active"] for c in cams)


def test_get_single_camera(admin_home_client, home_camera):
    resp = admin_home_client.get(f"/api/v1/cameras/{home_camera['id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == home_camera["id"]


def test_get_camera_404(admin_home_client):
    resp = admin_home_client.get(f"/api/v1/cameras/{uuid.uuid4()}")
    assert resp.status_code == 404


# ── Create ───────────────────────────────────────────────────────


def test_dept_admin_creates_camera_defaults_to_own_department(admin_home_client, home_dept_id):
    name = unique_camera_name()
    resp = admin_home_client.post(
        "/api/v1/cameras",
        json={"name": name, "connectivity_status": "offline"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == name
    assert body["department_id"] == home_dept_id


def test_dept_admin_cannot_create_camera_for_other_department(admin_home_client, rto_dept_id):
    resp = admin_home_client.post(
        "/api/v1/cameras",
        json={"name": unique_camera_name(), "department_id": rto_dept_id},
    )
    assert resp.status_code == 403


def test_create_camera_with_location_returns_geojson(admin_home_client):
    resp = admin_home_client.post(
        "/api/v1/cameras",
        json={
            "name": unique_camera_name(),
            "latitude": 23.0225,
            "longitude": 72.5714,
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["location"]["type"] == "Point"
    # GeoJSON coordinate order is [lon, lat]
    assert body["location"]["coordinates"] == pytest.approx([72.5714, 23.0225], abs=1e-6)


def test_create_camera_lat_without_lon_is_422(admin_home_client):
    resp = admin_home_client.post(
        "/api/v1/cameras",
        json={"name": unique_camera_name(), "latitude": 23.0},
    )
    assert resp.status_code == 422


def test_operator_cannot_create_camera(operator_client):
    resp = operator_client.post("/api/v1/cameras", json={"name": unique_camera_name()})
    assert resp.status_code == 403


# ── Update (PATCH) — this is the originally-reported "edit doesn't work" path ──


def test_dept_admin_updates_own_department_camera(admin_home_client, home_camera):
    resp = admin_home_client.patch(
        f"/api/v1/cameras/{home_camera['id']}",
        json={"name": "Renamed By Test", "connectivity_status": "maintenance"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Renamed By Test"
    assert body["connectivity_status"] == "maintenance"


def test_dept_admin_cannot_update_other_department_camera(admin_home_client, admin_rto_client, rto_camera):
    resp = admin_home_client.patch(
        f"/api/v1/cameras/{rto_camera['id']}",
        json={"name": "Should Not Be Allowed"},
    )
    assert resp.status_code == 403
    # And the camera must be unchanged.
    check = admin_rto_client.get(f"/api/v1/cameras/{rto_camera['id']}")
    assert check.json()["name"] == rto_camera["name"]


def test_update_camera_404_for_unknown_id(admin_home_client):
    resp = admin_home_client.patch(
        f"/api/v1/cameras/{uuid.uuid4()}", json={"name": "X"}
    )
    assert resp.status_code == 404


def test_update_writes_audit_trail(admin_home_client, home_camera):
    resp = admin_home_client.patch(
        f"/api/v1/cameras/{home_camera['id']}",
        json={"connectivity_status": "offline"},
    )
    assert resp.status_code == 200

    history = admin_home_client.get(f"/api/v1/cameras/{home_camera['id']}/history")
    assert history.status_code == 200
    rows = history.json()
    assert any(r["changed_field"] == "connectivity_status" for r in rows)


# ── The reported "genuine 403" on DELETE ───────────────────────────


def test_dept_admin_cannot_delete_other_department_camera(admin_home_client, admin_rto_client, rto_camera):
    resp = admin_home_client.delete(f"/api/v1/cameras/{rto_camera['id']}")
    assert resp.status_code == 403
    check = admin_rto_client.get(f"/api/v1/cameras/{rto_camera['id']}")
    assert check.json()["is_active"] is True


def test_dept_admin_can_soft_delete_own_camera(admin_home_client, home_camera):
    resp = admin_home_client.delete(f"/api/v1/cameras/{home_camera['id']}")
    assert resp.status_code == 200
    assert resp.json()["is_active"] is False

    # soft-deleted cameras drop out of the default (active-only) listing
    listing = admin_home_client.get("/api/v1/cameras").json()
    assert not any(c["id"] == home_camera["id"] for c in listing)

    # deleting again should now 400 (already deactivated)
    again = admin_home_client.delete(f"/api/v1/cameras/{home_camera['id']}")
    assert again.status_code == 400


# ── Bulk import ──────────────────────────────────────────────────


def test_bulk_import_creates_valid_rows_and_reports_errors(admin_home_client):
    csv_content = (
        "name,camera_type,ownership,connectivity_status\n"
        f"{unique_camera_name('Bulk A')},fixed,government,online\n"
        ",fixed,government,online\n"  # missing name -> error
    )
    files = {"file": ("cams.csv", csv_content, "text/csv")}
    resp = admin_home_client.post("/api/v1/cameras/bulk", files=files)
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 1
    assert body["errored"] == 1
    assert len(body["errors"]) == 1


def test_bulk_import_rejects_non_csv(admin_home_client):
    files = {"file": ("cams.txt", "name\nfoo\n", "text/plain")}
    resp = admin_home_client.post("/api/v1/cameras/bulk", files=files)
    assert resp.status_code == 400


def test_bulk_import_enforces_department_scope(admin_home_client, rto_dept_id):
    # RTO department name from seed.sql
    csv_content = (
        "name,department\n"
        f"{unique_camera_name('Bulk Cross Dept')},Regional Transport Office (RTO)\n"
    )
    files = {"file": ("cams.csv", csv_content, "text/csv")}
    resp = admin_home_client.post("/api/v1/cameras/bulk", files=files)
    assert resp.status_code == 200
    body = resp.json()
    assert body["created"] == 0
    assert body["errored"] == 1
