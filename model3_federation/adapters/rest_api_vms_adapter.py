"""
model3_federation.adapters.rest_api_vms_adapter
--------------------------------------------------
ONE class that onboards ANY VMS/webcam service that exposes:
  - a REST endpoint returning a JSON array (or array nested under a
    key) of camera-like objects
  - a single API key sent as a header

This is what makes "enter a base URL + API key + pick adapter type"
actually true for a whole category of integrations, not just Windy.
Windy is registered below as a PRESET (adapter_type="windy") of this
same class with its field paths pre-filled — proving the generic path
and the Windy-specific path are the same code, not two separate
implementations. Field-verified end to end with a real Windy API key
through the actual onboarding flow (POST /api/v3/systems/test-connection
→ connect() → get_cameras()), not just the MockTransport unit tests in
tests/test_rest_api_adapter.py — those cover the field-mapping logic,
this confirms the live path it's mapping against.

What this does NOT claim to solve: a REST API that needs OAuth,
pagination beyond a single page, or a response shape that isn't
"array of flat-ish objects" needs either extra config fields added
here or, past a certain point, its own adapter. This covers the
common case (Windy, and most cloud/hosted VMS + webcam-directory
REST APIs), not literally every API in existence — see registry.py's
module docstring for why "one adapter for every VMS" isn't a real
claim anyone should make.

Config fields (see REST_CONFIG_FIELDS below):
  base_url        e.g. https://api.windy.com/webcams/api/v3/webcams
  api_key         the secret
  api_key_header  header name the key is sent under (default: varies by preset)
  query_params    optional JSON string of extra query params, e.g.
                   '{"nearby": "23.0225,72.5714,200", "limit": "20", "include": "location,images"}'
  list_path       dotted path to the array in the response, "" = response is the array itself
                   (Windy preset: "webcams")
  id_field        dotted path to each item's unique id             (Windy: "webcamId")
  name_field      dotted path to each item's display name           (Windy: "title")
  lat_field       dotted path to latitude                           (Windy: "location.latitude")
  lng_field       dotted path to longitude                          (Windy: "location.longitude")
  location_field  dotted path to a human location string (optional) (Windy: "location.city")
  status_field    dotted path used to decide is_active (optional)   (Windy: "status", value "active")
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Optional

import httpx

from model3_federation.adapters.base import VMSAdapter
from model3_federation.adapters.registry import ConfigField, register_adapter_type
from model3_federation.schemas.models import FederatedCamera, FederatedEvent

logger = logging.getLogger("sentinel.federation.adapter.rest_api")

_POLL_INTERVAL_SECONDS = 300


def _dig(obj: dict, dotted_path: str) -> Any:
    """obj['location']['latitude'] via 'location.latitude'; missing → None."""
    if not dotted_path:
        return obj
    cur: Any = obj
    for part in dotted_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


class RestApiVMSAdapter(VMSAdapter):
    """Generic REST+API-key camera source, entirely config-driven."""

    def __init__(self, system_id: str, name: str, config: dict) -> None:
        self._system_id = system_id
        self._name = name
        self._config = config
        self._client = httpx.AsyncClient(timeout=10.0)
        self._connected = False

        try:
            self._query_params: dict = json.loads(config.get("query_params") or "{}")
        except json.JSONDecodeError:
            self._query_params = {}

    @property
    def system_name(self) -> str:
        return self._name

    @property
    def vendor(self) -> str:
        return self._config.get("vendor_label", "REST API")

    @property
    def system_id(self) -> str:
        return self._system_id

    def _headers(self) -> dict:
        header_name = self._config.get("api_key_header", "x-api-key")
        return {header_name: self._config["api_key"]}

    async def connect(self) -> bool:
        from model3_federation.adapters.registry import resolve_safe_url
        base_url = self._config.get("base_url")
        api_key = self._config.get("api_key")
        if not base_url or not api_key:
            self.log_error("Missing base_url or api_key in config — cannot connect.")
            return False
            
        safe, pinned_url, host = resolve_safe_url(base_url)
        if not safe:
            self.log_error("SSRF Protection: blocked attempt to connect to unsafe or private URL.")
            return False
            
        try:
            headers = self._headers()
            if host:
                headers["Host"] = host
            resp = await self._client.get(pinned_url, headers=headers, params=self._query_params)
            resp.raise_for_status()
            self._connected = True
            self.log_info(f"Connected to {base_url}")
            return True
        except httpx.HTTPStatusError as exc:
            self.log_error(f"{base_url} rejected the request: {exc.response.status_code} {exc.response.text[:200]}")
            return False
        except httpx.RequestError as exc:
            self.log_error(f"Could not reach {base_url}: {exc}")
            return False

    async def get_cameras(self) -> list[FederatedCamera]:
        if not self._connected:
            return []
        
        from model3_federation.adapters.registry import resolve_safe_url
        base_url = self._config["base_url"]
        safe, pinned_url, host = resolve_safe_url(base_url)
        if not safe:
            self.log_error("SSRF Protection: blocked attempt to connect to unsafe or private URL.")
            return []
            
        try:
            headers = self._headers()
            if host:
                headers["Host"] = host
            resp = await self._client.get(pinned_url, headers=headers, params=self._query_params)
            resp.raise_for_status()
            payload = resp.json()
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            self.log_error(f"Failed to fetch camera list: {exc}")
            return []

        items = _dig(payload, self._config.get("list_path", "")) or []
        if not isinstance(items, list):
            self.log_error(f"list_path {self._config.get('list_path')!r} did not resolve to a list.")
            return []

        id_field = self._config.get("id_field", "id")
        name_field = self._config.get("name_field", "name")
        lat_field = self._config.get("lat_field", "")
        lng_field = self._config.get("lng_field", "")
        location_field = self._config.get("location_field", "")
        status_field = self._config.get("status_field", "")

        cameras: list[FederatedCamera] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            ext_id = _dig(item, id_field)
            if ext_id is None:
                continue
            is_active = True
            if status_field:
                is_active = _dig(item, status_field) == self._config.get("active_status_value", "active")
            cameras.append(FederatedCamera(
                external_id=str(ext_id),
                name=str(_dig(item, name_field) or f"Camera {ext_id}"),
                system_name=self._name,
                vendor=self.vendor,
                department=self._config.get("department_hint", "External"),
                lat=_dig(item, lat_field) if lat_field else None,
                lng=_dig(item, lng_field) if lng_field else None,
                location_label=str(_dig(item, location_field)) if location_field and _dig(item, location_field) else None,
                is_active=is_active,
            ))
        return cameras

    async def start_event_stream(
        self,
        callback: Callable[[FederatedEvent], Awaitable[None]],
    ) -> None:
        """No generic REST API has a standard detection-event format,
        so this emits a periodic 'camera_heartbeat' per camera — an
        honest "this source is alive, here's its current inventory"
        signal, same pattern as the Windy-specific adapter it replaces."""
        self.log_info("REST API refresh loop started.")
        while True:
            try:
                await asyncio.sleep(_POLL_INTERVAL_SECONDS)
                for cam in await self.get_cameras():
                    await callback(FederatedEvent(
                        system_id=self._system_id,
                        system_name=self._name,
                        vendor=self.vendor,
                        camera_external_id=cam.external_id,
                        camera_name=cam.name,
                        event_type="camera_heartbeat",
                        raw_payload={"source": "rest_api_adapter"},
                    ))
            except asyncio.CancelledError:
                self.log_info("Event stream cancelled.")
                raise
            except Exception as exc:
                self.log_error(f"Unexpected error in refresh loop: {exc}")
                await asyncio.sleep(5.0)

    async def aclose(self) -> None:
        await self._client.aclose()


REST_CONFIG_FIELDS = [
    ConfigField("base_url", "Base URL", "url", True,
                help_text="Full endpoint that returns your camera list, e.g. https://api.example.com/cameras"),
    ConfigField("api_key", "API Key", "password", True),
    ConfigField("api_key_header", "API Key Header Name", "text", False, "x-api-key"),
    ConfigField("query_params", "Extra Query Params (JSON, optional)", "text", False, "{}"),
    ConfigField("list_path", "Path to camera array in response (blank = top-level array)", "text", False, ""),
    ConfigField("id_field", "Field: camera ID", "text", True, "id"),
    ConfigField("name_field", "Field: camera name", "text", True, "name"),
    ConfigField("lat_field", "Field: latitude (optional)", "text", False, ""),
    ConfigField("lng_field", "Field: longitude (optional)", "text", False, ""),
    ConfigField("location_field", "Field: location label (optional)", "text", False, ""),
    ConfigField("status_field", "Field: status (optional)", "text", False, ""),
    ConfigField("department_hint", "Department hint (optional)", "text", False, "External"),
]

register_adapter_type(
    adapter_type="rest_api",
    display_name="Generic REST API + API Key",
    config_fields=REST_CONFIG_FIELDS,
)(RestApiVMSAdapter)


class WindyWebcamsAdapter(RestApiVMSAdapter):
    """Windy preset — same RestApiVMSAdapter, field paths pre-filled so
    the onboarding form only asks for lat/lng/radius/key, not raw
    dotted-path field names. This is the proof that 'generic' and
    'Windy' aren't two different code paths."""

    def __init__(self, system_id: str, name: str, config: dict) -> None:
        lat = config.get("lat", "23.0225")
        lng = config.get("lng", "72.5714")
        radius = config.get("radius_km", "200")
        limit = config.get("limit", "20")
        merged = {
            "base_url": "https://api.windy.com/webcams/api/v3/webcams",
            "api_key": config.get("api_key", ""),
            "api_key_header": "x-windy-api-key",
            "query_params": json.dumps({
                "nearby": f"{lat},{lng},{radius}",
                "limit": limit,
                "include": "location,images",
            }),
            "list_path": "webcams",
            "id_field": "webcamId",
            "name_field": "title",
            "lat_field": "location.latitude",
            "lng_field": "location.longitude",
            "location_field": "location.city",
            "status_field": "status",
            "active_status_value": "active",
            "department_hint": "External / Public Webcam",
            "vendor_label": "Windy.com",
        }
        super().__init__(system_id, name, merged)


register_adapter_type(
    adapter_type="windy",
    display_name="Windy.com Public Webcams",
    config_fields=[
        ConfigField("api_key", "Windy API Key", "password", True),
        ConfigField("lat", "Latitude", "text", True, "23.0225"),
        ConfigField("lng", "Longitude", "text", True, "72.5714"),
        ConfigField("radius_km", "Search radius (km, max 250)", "number", False, "200"),
        ConfigField("limit", "Max cameras", "number", False, "20"),
    ],
)(WindyWebcamsAdapter)
