import pytest


def _department_id_by_name_fragment(client, fragment: str) -> str:
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
    cams = admin_home_client.get(
        "/api/v1/cameras", params={"department_id": home_dept_id}
    ).json()
    return cams[0]


@pytest.fixture()
def rto_camera(admin_rto_client, rto_dept_id):
    cams = admin_rto_client.get(
        "/api/v1/cameras", params={"department_id": rto_dept_id}
    ).json()
    return cams[0]


def test_anonymous_redirected_to_login(anon_client):
    resp = anon_client.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login"


def test_alerts_placeholder_anonymous_redirected_to_login(anon_client):
    """Regression guard for AuditReport1.md finding 20 / 4.2: the /alerts
    placeholder page was the one page in this router with no login guard,
    unlike every sibling page above."""
    resp = anon_client.get("/alerts", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login"


def test_alerts_placeholder_loads_for_authenticated_user(admin_home_client):
    resp = admin_home_client.get("/alerts")
    assert resp.status_code == 200
    assert "Alerts" in resp.text


def test_camera_edit_form_own_department_is_editable(admin_home_client, home_camera):
    resp = admin_home_client.get(f"/cameras/{home_camera['id']}/edit")
    assert resp.status_code == 200
    html = resp.text
    assert 'data-editable="true"' in html
    assert 'name="latitude"' in html
    # Save button should be present (only rendered when editable)
    assert "Save Changes" in html


def test_camera_edit_form_other_department_is_view_only(admin_home_client, rto_camera):
    """This is the exact scenario behind the originally-reported bug: a
    dept_admin opening another department's camera should get a clearly
    labelled read-only view, not a form that silently 403s on submit."""
    resp = admin_home_client.get(f"/cameras/{rto_camera['id']}/edit")
    assert resp.status_code == 200
    html = resp.text
    assert 'data-editable="false"' in html
    assert "not your assigned department" in html.lower()
    assert "Save Changes" not in html


def test_camera_edit_form_department_dropdown_scoped_to_own_department(
    admin_home_client, home_camera, home_dept_id
):
    resp = admin_home_client.get(f"/cameras/{home_camera['id']}/edit")
    html = resp.text
    # Editable form -> placeholder "— Select —" option + exactly the
    # dept_admin's own department (never every department in the system).
    dept_select_start = html.index('id="cam-department"')
    dept_select_end = html.index("</select>", dept_select_start)
    dept_html = html[dept_select_start:dept_select_end]
    assert dept_html.count("<option") == 2
    assert home_dept_id in dept_html


def test_camera_edit_form_district_dropdown_has_all_33_districts(admin_home_client, home_camera):
    """Regression test for the empty-districts-table bug: the district
    dropdown must be populated (33 real Gujarat districts from seed.sql),
    not just the placeholder option."""
    resp = admin_home_client.get(f"/cameras/{home_camera['id']}/edit")
    html = resp.text
    dist_select_start = html.index('id="cam-district"')
    dist_select_end = html.index("</select>", dist_select_start)
    dist_html = html[dist_select_start:dist_select_end]
    # placeholder + 33 real districts
    assert dist_html.count("<option") == 34


def test_camera_edit_form_404_for_unknown_camera(admin_home_client):
    import uuid

    resp = admin_home_client.get(f"/cameras/{uuid.uuid4()}/edit")
    assert resp.status_code == 404


def test_camera_new_form_scopes_department_for_dept_admin(admin_home_client, home_dept_id):
    resp = admin_home_client.get("/cameras/new")
    assert resp.status_code == 200
    html = resp.text
    dept_select_start = html.index('id="cam-department"')
    dept_select_end = html.index("</select>", dept_select_start)
    dept_html = html[dept_select_start:dept_select_end]
    assert dept_html.count("<option") == 2  # "— Select —" + own dept only
    assert home_dept_id in dept_html


def test_non_dept_admin_redirected_away_from_camera_forms(operator_client, viewer_client):
    for c in (operator_client, viewer_client):
        resp = c.get("/cameras/new", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["location"] == "/cameras"


def test_map_page_loads_for_authenticated_user(admin_home_client):
    resp = admin_home_client.get("/")
    assert resp.status_code == 200
    assert "map" in resp.text.lower()


def test_districts_page_loads(admin_home_client):
    resp = admin_home_client.get("/districts")
    assert resp.status_code == 200


def test_gap_analysis_page_loads(admin_home_client):
    resp = admin_home_client.get("/gap-analysis")
    assert resp.status_code == 200
