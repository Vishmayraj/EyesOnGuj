"""
Unit tests for model3_federation/adapters/rest_api_vms_adapter.py.

No DB, no real network: httpx.MockTransport (built into httpx, already a
test dependency via requirements-dev.txt -- no new dependency added for
this) stands in for the actual HTTP call, so these test the adapter's
own logic (field-mapping via dotted paths, connect() success/failure,
the Windy preset) against a canned response instead of hitting
api.windy.com. The Windy probe script (windy_probe.py, handed over
separately) is what verified the real API; this verifies the adapter
built on top of it.

Same "wrap in asyncio.run()" convention as test_registration.py -- no
pytest-asyncio anywhere in this repo.
"""

import asyncio
import json

import httpx
import pytest

from model3_federation.adapters.rest_api_vms_adapter import RestApiVMSAdapter, WindyWebcamsAdapter
from model3_federation.schemas.models import FederatedEvent


def _run(coro):
    return asyncio.run(coro)


def _install_mock_transport(monkeypatch, handler):
    """Replace httpx.AsyncClient (as seen by rest_api_vms_adapter's own
    `import httpx`) with one wired to a MockTransport, for the duration
    of one test. Reverted automatically by the monkeypatch fixture."""
    real_async_client = httpx.AsyncClient

    def _fake_async_client(*args, **kwargs):
        kwargs.pop("timeout", None)
        return real_async_client(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(httpx, "AsyncClient", _fake_async_client)


# ── Generic REST adapter, arbitrary field mapping ───────────────────────

def _generic_handler(request: httpx.Request) -> httpx.Response:
    assert request.headers.get("x-api-key") == "secret-key"
    return httpx.Response(200, json={
        "results": [
            {"cam_id": "C1", "label": "Front Door", "pos": {"lat": 12.9, "lng": 77.5}, "state": "up"},
            {"cam_id": "C2", "label": "Back Yard", "pos": {"lat": 12.8, "lng": 77.6}, "state": "down"},
        ]
    })


def _generic_adapter(monkeypatch, handler=_generic_handler) -> RestApiVMSAdapter:
    _install_mock_transport(monkeypatch, handler)
    monkeypatch.setattr("model3_federation.adapters.rest_api_vms_adapter.resolve_safe_url", lambda x: (True, x, "example.invalid"))
    return RestApiVMSAdapter("sys-1", "Generic Test VMS", {
        "base_url": "https://example.invalid/cameras",
        "api_key": "secret-key",
        "api_key_header": "x-api-key",
        "list_path": "results",
        "id_field": "cam_id",
        "name_field": "label",
        "lat_field": "pos.lat",
        "lng_field": "pos.lng",
        "status_field": "state",
        "active_status_value": "up",
        "department_hint": "External",
    })


def test_connect_success(monkeypatch):
    adapter = _generic_adapter(monkeypatch)
    assert _run(adapter.connect()) is True


def test_connect_failure_on_http_error(monkeypatch):
    def handler(request):
        return httpx.Response(401, json={"error": "bad key"})
    adapter = _generic_adapter(monkeypatch, handler)
    assert _run(adapter.connect()) is False


def test_connect_failure_missing_config():
    adapter = RestApiVMSAdapter("sys-1", "No Config VMS", {})
    assert _run(adapter.connect()) is False


def test_get_cameras_before_connect_returns_empty(monkeypatch):
    adapter = _generic_adapter(monkeypatch)
    assert _run(adapter.get_cameras()) == []


def test_get_cameras_maps_dotted_fields_correctly(monkeypatch):
    adapter = _generic_adapter(monkeypatch)
    _run(adapter.connect())
    cameras = _run(adapter.get_cameras())

    assert [c.external_id for c in cameras] == ["C1", "C2"]
    assert [c.name for c in cameras] == ["Front Door", "Back Yard"]
    assert cameras[0].lat == 12.9 and cameras[0].lng == 77.5
    assert cameras[0].is_active is True   # state == active_status_value ("up")
    assert cameras[1].is_active is False  # state == "down"
    assert all(c.system_name == "Generic Test VMS" for c in cameras)
    assert all(c.department == "External" for c in cameras)


def test_get_cameras_empty_result_is_not_an_error(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={"results": []})
    adapter = _generic_adapter(monkeypatch, handler)
    _run(adapter.connect())
    assert _run(adapter.get_cameras()) == []


def test_get_cameras_skips_items_missing_id_field(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={"results": [
            {"cam_id": "C1", "label": "Has ID"},
            {"label": "No ID Field"},
        ]})
    adapter = _generic_adapter(monkeypatch, handler)
    _run(adapter.connect())
    cameras = _run(adapter.get_cameras())
    assert [c.external_id for c in cameras] == ["C1"]


def test_get_cameras_bad_list_path_logs_and_returns_empty(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={"results": {"not": "a list"}})
    adapter = _generic_adapter(monkeypatch, handler)
    _run(adapter.connect())
    assert _run(adapter.get_cameras()) == []


# ── Windy preset: same class, pre-filled field paths ────────────────────

def _windy_response(request: httpx.Request) -> httpx.Response:
    assert request.headers.get("x-windy-api-key") == "windy-test-key"
    return httpx.Response(200, json={
        "webcams": [
            {
                "webcamId": 111,
                "title": "Lauterbrunnen: Ostgrat",
                "status": "active",
                "location": {"latitude": 46.59, "longitude": 7.91, "city": "Lauterbrunnen", "country": "Switzerland"},
            },
            {
                "webcamId": 222,
                "title": "Old Cam",
                "status": "inactive",
                "location": {"latitude": 46.5, "longitude": 8.0, "city": "Fiesch", "country": "Switzerland"},
            },
        ]
    })


def test_windy_preset_connects_and_maps_real_field_shape(monkeypatch):
    _install_mock_transport(monkeypatch, _windy_response)
    adapter = WindyWebcamsAdapter("sys-windy", "Windy Public Webcams", {
        "api_key": "windy-test-key", "lat": "46.54", "lng": "7.98", "radius_km": "20",
    })
    assert _run(adapter.connect()) is True
    cameras = _run(adapter.get_cameras())

    assert [c.external_id for c in cameras] == ["111", "222"]
    assert cameras[0].name == "Lauterbrunnen: Ostgrat"
    assert cameras[0].lat == 46.59 and cameras[0].lng == 7.91
    assert cameras[0].location_label == "Lauterbrunnen"
    assert cameras[0].is_active is True    # status == "active"
    assert cameras[1].is_active is False   # status == "inactive"
    assert all(c.vendor == "Windy.com" for c in cameras)


def test_windy_preset_is_same_class_as_generic_rest_adapter():
    """The whole point of the preset: proving 'generic' and 'Windy' are
    the same implementation, not two adapters that happen to look similar."""
    assert issubclass(WindyWebcamsAdapter, RestApiVMSAdapter)


def test_windy_preset_registered_adapter_type():
    assert WindyWebcamsAdapter.adapter_type == "windy"
    assert RestApiVMSAdapter.adapter_type == "rest_api"


# ── Event stream: heartbeat loop, cancellation ──────────────────────────

def test_event_stream_emits_heartbeat_per_camera_then_cancels_cleanly(monkeypatch):
    import model3_federation.adapters.rest_api_vms_adapter as mod
    monkeypatch.setattr(mod, "_POLL_INTERVAL_SECONDS", 0)  # don't actually wait 5 minutes in a test

    adapter = _generic_adapter(monkeypatch)
    _run(adapter.connect())

    received: list[FederatedEvent] = []

    async def _callback(event: FederatedEvent) -> None:
        received.append(event)

    async def _scenario():
        task = asyncio.create_task(adapter.start_event_stream(_callback))
        # Let the loop run one full iteration (sleep(0) + one get_cameras() + 2 callbacks).
        for _ in range(50):
            await asyncio.sleep(0)
            if len(received) >= 2:
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    _run(_scenario())

    assert len(received) >= 2
    assert {e.camera_external_id for e in received} >= {"C1", "C2"}
    assert all(e.event_type == "camera_heartbeat" for e in received)
    assert all(e.system_id == "sys-1" for e in received)
