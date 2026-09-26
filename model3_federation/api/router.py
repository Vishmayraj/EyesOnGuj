"""
model3_federation.api.router
------------------------------
FastAPI router for the Model 3 Federation layer.

Mounts at /api/v3 in model1-registry/app/main.py (one added line).
Also provides the WebSocket endpoint /ws/federation used by the
federation.html dashboard.

At startup (lifespan hook in main.py) the three VMS adapters are
started as asyncio background tasks and the correlation engine is
wired in. This router also manages the WebSocket connection registry
and provides the ws_broadcast() callback to the engine.

All REST endpoints are read-only GET requests (plus one POST
for acknowledge and one POST for simulate-burst). No existing
Model 1 routes, models, or schemas are modified.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user, require_role
from shared.db.models import User as UserModel
from shared.db.session import get_db
from shared.security import encrypt_config
from model3_federation.bus.event_bus import FederationEventBus
from model3_federation.correlation.engine import CorrelationEngine
from model3_federation.registration import register_adapter, register_adapter_in_request, load_dynamic_adapters
from model3_federation.adapters.police_vms_adapter import PoliceVMSAdapter
from model3_federation.adapters.rto_vms_adapter import RTOVMSAdapter
from model3_federation.adapters.municipal_vms_adapter import MunicipalVMSAdapter
# Importing these registers their adapter types into the registry
# (registry.register_adapter_type is a decorator that runs at import
# time) — see adapters/registry.py. Adding a new config-driven adapter
# type means adding one class + one import here; it does NOT mean
# editing _adapters below or any onboarding endpoint.
import model3_federation.adapters.rest_api_vms_adapter  # noqa: F401  (windy, rest_api)
import model3_federation.adapters.onvif_vms_adapter  # noqa: F401  (onvif)
from model3_federation.adapters.registry import build_adapter, list_adapter_types, validate_config
from model3_federation.schemas.models import FederatedEvent, VMSSystemCreate, VMSSystemUpdate, WSMessage

logger = logging.getLogger("sentinel.federation.api")

router = APIRouter(prefix="/api/v3", tags=["federation"])

# ── Module-level singletons (initialised in start_federation_services) ─────

_bus: Optional[FederationEventBus] = None
_engine: Optional[CorrelationEngine] = None
_adapters: list = []
_tasks: list[asyncio.Task] = []

# WebSocket connection registry: set of active WebSocket clients
_ws_clients: set[WebSocket] = set()

# In-memory rate counter: events per minute per system (for topology cards)
_events_per_min: dict[str, list[datetime]] = {}


# ── WebSocket broadcast helper ───────────────────────────────────────────────

async def _ws_broadcast(message: WSMessage) -> None:
    """Broadcast a WSMessage to all connected WebSocket clients."""
    dead: set[WebSocket] = set()
    payload = message.model_dump_json()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)


# ── Rate tracker helper ──────────────────────────────────────────────────────

def _record_event_rate(system_id: str) -> None:
    now = datetime.now(tz=timezone.utc)
    bucket = _events_per_min.setdefault(system_id, [])
    bucket.append(now)
    # Keep only the last 60 seconds
    cutoff = now - timedelta(seconds=60)
    _events_per_min[system_id] = [t for t in bucket if t >= cutoff]


# ── Federation lifecycle ─────────────────────────────────────────────────────

def _on_task_done(task: asyncio.Task) -> None:
    """
    Log a background task's failure without touching its siblings.
    Each adapter and the correlation engine run as independent
    fire-and-forget tasks — one dying (e.g. a single bad VMS adapter)
    should not take down the others.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Federation task %r crashed: %s", task.get_name(), exc, exc_info=exc)


async def start_federation_services(db_session_factory, redis_url: str) -> None:
    """
    Called from model1-registry/app/main.py lifespan on startup.
    Initialises the event bus, correlation engine, and all three VMS adapters,
    registers each adapter's reported system/camera inventory into the DB,
    then starts each as its own independent background task and returns —
    it does not block waiting on them. Call stop_federation_services() on
    shutdown to cancel everything cleanly.
    """
    global _bus, _engine, _adapters, _tasks

    _bus = FederationEventBus(redis_url=redis_url)

    # Wrap bus publish to also track event rate per system
    _original_publish = _bus.publish

    async def _tracked_publish(event: FederatedEvent) -> None:
        _record_event_rate(event.system_id)
        await _original_publish(event)

    _bus.publish = _tracked_publish  # type: ignore[method-assign]

    _engine = CorrelationEngine(
        bus=_bus,
        db_session_factory=db_session_factory,
        ws_broadcast=_ws_broadcast,
    )

    _adapters = [PoliceVMSAdapter(), RTOVMSAdapter(), MunicipalVMSAdapter()]
    _tasks = []

    engine_task = asyncio.create_task(_engine.start(), name="federation-engine")
    engine_task.add_done_callback(_on_task_done)
    _tasks.append(engine_task)

    # Connect, register, and start each adapter's event stream independently.
    for adapter in _adapters:
        connected = await adapter.connect()
        if not connected:
            logger.error("Adapter failed to connect: %s", adapter.system_name)
            continue

        await register_adapter(db_session_factory, adapter)

        task = asyncio.create_task(
            adapter.start_event_stream(_bus.publish),
            name=f"federation-adapter-{adapter.vendor}",
        )
        task.add_done_callback(_on_task_done)
        _tasks.append(task)
        logger.info("Adapter started: %s", adapter.system_name)

    # Config-driven systems (POST /systems with adapter_type + config —
    # Windy, ONVIF, or any future registry.py adapter type) get restored
    # here so a restart doesn't quietly drop everything an operator
    # onboarded through the UI. These run exactly the same way as the
    # three demo adapters above from this point on (same task pattern,
    # same event bus) — the only difference is where their config came
    # from (DB row vs hardcoded constructor).
    dynamic_adapters = await load_dynamic_adapters(db_session_factory)
    for adapter in dynamic_adapters:
        _adapters.append(adapter)
        task = asyncio.create_task(
            adapter.start_event_stream(_bus.publish),
            name=f"federation-adapter-{adapter.vendor}-{adapter.system_id[:8]}",
        )
        task.add_done_callback(_on_task_done)
        _tasks.append(task)
        logger.info("Dynamic adapter started: %s", adapter.system_name)

    logger.info("Model 3 Federation services started. %d adapters running.", len(_adapters))


async def stop_federation_services() -> None:
    """Cancel all federation background tasks. Called from main.py lifespan on shutdown."""
    global _tasks
    for task in _tasks:
        task.cancel()
    for task in _tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            # Already logged by _on_task_done; swallow here so shutdown proceeds.
            pass
    _tasks = []


# ── WebSocket endpoint ───────────────────────────────────────────────────────

@router.websocket("/ws/federation")
async def ws_federation(websocket: WebSocket, db: Session = Depends(get_db)):
    """
    WebSocket endpoint for the live federation dashboard.
    Pushes FederatedEvent, FederatedAlert, and CorrelationResult objects
    as JSON to every connected browser client.

    Auth reuses get_current_user - the same cookie/header check every
    REST endpoint below uses - but calls it directly rather than wiring
    it up as Depends(get_current_user) on this function's own signature.
    This FastAPI/Starlette version does not substitute a WebSocket for a
    Request-typed dependency on a websocket route: get_current_user takes
    request: Request, so a Depends(get_current_user) parameter here (as
    a previous version of this endpoint had) is left unfulfilled, and
    FastAPI's own dependency solver raises a bare
    "get_current_user() missing 1 required positional argument: 'request'"
    TypeError from inside the ASGI app - crashing the connection for
    every caller regardless of auth, not rejecting it with a clean 401
    the way the equivalent REST-endpoint case does.

    Calling get_current_user directly sidesteps FastAPI's dependency
    solver for this one call: Python does not check the request: Request
    annotation at runtime, and the only things get_current_user (via
    get_token_from_request) touches on that object - .cookies, .headers -
    exist on WebSocket too, since Request and WebSocket both derive from
    Starlette's HTTPConnection. `db` still comes through Depends(get_db)
    normally on this function's own signature, which works fine here
    since get_db takes no Request/WebSocket-typed parameter to resolve.
    """
    try:
        current_user = get_current_user(websocket, db)
    except HTTPException:
        logger.warning("Unauthenticated WebSocket connection to /ws/federation - rejecting cleanly.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Unauthorized")
        return

    await websocket.accept()
    _ws_clients.add(websocket)
    logger.info("WebSocket client connected. Total: %d", len(_ws_clients))

    # Send initial heartbeat so the client knows it's connected
    try:
        await websocket.send_text(json.dumps({
            "type": "heartbeat",
            "payload": {
                "message": "Federation WebSocket connected",
                "adapter_count": len(_adapters),
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            }
        }))
    except Exception:
        pass

    try:
        while True:
            # Keep the connection alive; the engine pushes messages via _ws_broadcast
            await asyncio.sleep(30)
            try:
                await websocket.send_text(json.dumps({
                    "type": "heartbeat",
                    "payload": {"timestamp": datetime.now(tz=timezone.utc).isoformat()}
                }))
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        logger.info("WebSocket client disconnected. Total: %d", len(_ws_clients))


# ── REST endpoints ───────────────────────────────────────────────────────────

@router.get("/systems")
def get_federated_systems(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """List all VMS systems (main grid + federated) with status, camera count, and last heartbeat."""
    rows = db.execute(text(
        """
        SELECT vs.id, vs.name, vs.vendor, vs.status,
               vs.camera_count, vs.last_heartbeat, vs.protocol, vs.ownership,
               d.name AS department_name, vs.department_hint, vs.adapter_type
        FROM   vms_systems vs
        LEFT JOIN departments d ON d.id = vs.department_id
        ORDER  BY vs.name
        """
    )).fetchall()

    result = []
    for r in rows:
        sys_id = str(r[0])
        epm_bucket = _events_per_min.get(sys_id, [])
        department_name = r[8]
        # department_name is NULL exactly when vs.department_id is NULL
        # (departments can't be deleted while referenced - see triggers.sql),
        # so this is a direct proxy for "department_id IS NULL". A hint
        # only shows up here when there's also one on file (registration.py
        # records it during adapter self-registration) and it's the reason
        # department is unset, not just an unset field on a manually
        # onboarded system that was never given a department on purpose.
        result.append({
            "id":                       sys_id,
            "name":                     r[1],
            "vendor":                   r[2],
            "status":                   r[3],
            "camera_count":             r[4] or 0,
            "last_heartbeat":           r[5].isoformat() if r[5] else None,
            "protocol":                 r[6],
            "ownership":                r[7],
            "department":               department_name,
            "unmatched_department_hint": r[9] if department_name is None else None,
            "events_per_min":           len(epm_bucket),
            # Present exactly when this row has a live adapter behind it
            # (config-driven onboarding, POST /systems with adapter_type) —
            # distinct from `protocol`, which is set to the same string
            # for these rows but is also free text on old/manual rows, so
            # it alone can't be used to tell "live" apart from "on file".
            "adapter_type":             r[10],
        })
    return result


# ── Adapter types (config-driven onboarding) ────────────────────────────────
#
# Drives the onboarding form: pick a type, get that type's exact fields,
# nothing hardcoded in the frontend either. See adapters/registry.py.

@router.get("/adapter-types")
def get_adapter_types(
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    return list_adapter_types()


@router.post("/systems/test-connection")
async def test_connection(
    payload: VMSSystemCreate,
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
) -> dict[str, Any]:
    """
    The [Test Connection] button: builds a throwaway adapter from
    adapter_type + config and actually calls connect() + get_cameras()
    against the real system. Nothing is persisted — this exists so a
    wrong API key or unreachable host fails loudly on this screen
    instead of silently producing another 'disconnected' row.
    """
    if not payload.adapter_type:
        raise HTTPException(status_code=400, detail="adapter_type is required to test a connection.")

    errors = validate_config(payload.adapter_type, payload.config or {})
    if errors:
        raise HTTPException(status_code=400, detail={"config_errors": errors})

    adapter = build_adapter(payload.adapter_type, str(uuid4()), payload.name, payload.config or {})
    connected = await adapter.connect()
    if not connected:
        return {"success": False, "message": "Could not connect — check the config and try again.", "camera_count": 0}

    cameras = await adapter.get_cameras()
    if hasattr(adapter, "aclose"):
        await adapter.aclose()

    return {
        "success": True,
        "message": f"Connected. Found {len(cameras)} camera(s).",
        "camera_count": len(cameras),
        "sample_cameras": [{"name": c.name, "external_id": c.external_id} for c in cameras[:5]],
    }


# ── VMS onboarding (Finding 1 + generic connector framework) ──────────────
#
# Two ways a vms_systems row gets created:
#   1. Adapter self-registration (register_adapter(), called from
#      start_federation_services() at startup) — Police/RTO/Municipal,
#      hardcoded, always 'connected'.
#   2. This endpoint — which itself now branches on whether the caller
#      supplied adapter_type + config:
#        - WITHOUT adapter_type: exactly the original record-only
#          behavior. Created 'disconnected', camera_count 0, stays that
#          way forever. Still exists for VMS integrations with no
#          adapter written for them at all (e.g. a vendor who only ever
#          emails CSVs) — a dept_admin can still put it on file.
#        - WITH adapter_type: actually connects (same registry.py
#          adapter used by /systems/test-connection above), pulls its
#          real camera list, saves adapter_type+config so
#          load_dynamic_adapters() brings it back on every restart, and
#          starts its background event-stream task immediately — no
#          redeploy, no editing start_federation_services(). This is
#          the "enter a base URL/host + API key or credentials + pick a
#          type, see the real cameras" path.
#
# Same role gate either way: dept_admin or operator.

@router.post("/systems", status_code=201)
async def create_system(
    payload: VMSSystemCreate,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
) -> dict[str, Any]:
    # IDOR Fix: Force department to match the user's department for scoped admins
    if current_user.department_id:
        if payload.department_id is not None and str(payload.department_id) != str(current_user.department_id):
            raise HTTPException(status_code=403, detail="You can only create systems for your own department.")
        payload.department_id = current_user.department_id

    if payload.department_id is not None:
        dept_row = db.execute(text(
            "SELECT 1 FROM departments WHERE id = :id"
        ), {"id": payload.department_id}).fetchone()
        if dept_row is None:
            raise HTTPException(
                status_code=400,
                detail=f"department_id {payload.department_id!r} does not exist",
            )

    system_id = str(uuid4())

    if not payload.adapter_type:
        # Original record-only path, unchanged.
        db.execute(text(
            """
            INSERT INTO vms_systems (id, name, vendor, protocol, ownership, department_id, status, camera_count)
            VALUES (:id, :name, :vendor, :protocol, :ownership, :dept, 'disconnected', 0)
            """
        ), {
            "id": system_id, "name": payload.name, "vendor": payload.vendor,
            "protocol": payload.protocol, "ownership": payload.ownership, "dept": payload.department_id,
        })
        db.commit()
        return {
            "id": system_id, "name": payload.name, "vendor": payload.vendor,
            "protocol": payload.protocol, "ownership": payload.ownership,
            "department_id": payload.department_id, "status": "disconnected", "camera_count": 0,
        }

    # Config-driven path.
    errors = validate_config(payload.adapter_type, payload.config or {})
    if errors:
        raise HTTPException(status_code=400, detail={"config_errors": errors})

    adapter = build_adapter(payload.adapter_type, system_id, payload.name, payload.config or {})
    connected = await adapter.connect()

    db.execute(text(
        """
        INSERT INTO vms_systems
          (id, name, vendor, protocol, adapter_type, config, ownership, department_id, status, camera_count)
        VALUES
          (:id, :name, :vendor, :protocol, :adapter_type, :config, :ownership, :dept, :status, 0)
        """
    ), {
        "id": system_id,
        "name": payload.name,
        "vendor": payload.vendor or adapter.vendor,
        "protocol": payload.adapter_type,
        "adapter_type": payload.adapter_type,
        "config": encrypt_config(payload.config or {}),
        "ownership": payload.ownership,
        "dept": payload.department_id,
        "status": "connected" if connected else "disconnected",
    })
    db.commit()

    if not connected:
        if hasattr(adapter, "aclose"):
            await adapter.aclose()
        return {
            "id": system_id, "name": payload.name, "adapter_type": payload.adapter_type,
            "status": "disconnected", "camera_count": 0,
            "message": "Saved, but could not connect. Edit and re-test — this will retry on next restart too.",
        }

    # Connected: pull its real camera list now and upsert into the
    # SAME request session/transaction the INSERT above used (see
    # register_adapter_in_request's docstring for why this must not
    # open a second connection via _SessionLocal — it did originally,
    # and that's exactly the isolation hazard conftest.py's
    # DISABLE_FEDERATION_STARTUP comment warns about for the startup
    # path; this endpoint has no such flag to protect it), then start
    # its live event stream immediately rather than waiting for the
    # next process restart.
    await register_adapter_in_request(db, adapter, ownership=payload.ownership)
    db.commit()

    # _bus is only set once start_federation_services() has actually run
    # (main.py's lifespan hook — skipped entirely when
    # DISABLE_FEDERATION_STARTUP=true, which the test suite always sets,
    # see model1-registry/tests/conftest.py). Camera registration above
    # still happens either way; only the live event-stream task needs a
    # running bus to publish into. Previously this unconditionally did
    # `adapter.start_event_stream(_bus.publish)`, which is an
    # AttributeError on None — caught by writing
    # test_dynamic_onboarding_api.py, not by any manual check.
    if _bus is not None:
        task = asyncio.create_task(
            adapter.start_event_stream(_bus.publish),
            name=f"federation-adapter-{adapter.vendor}-{system_id[:8]}",
        )
        task.add_done_callback(_on_task_done)
        _tasks.append(task)
        _adapters.append(adapter)
    else:
        logger.warning(
            "Federation event bus isn't running (federation startup disabled) — "
            "%s was registered and its cameras saved, but its live event stream "
            "was not started. It will start normally on the next full app startup.",
            payload.name,
        )

    return {
        "id": system_id, "name": payload.name, "adapter_type": payload.adapter_type,
        "status": "connected",
        "message": "Connected. Cameras will populate within a few seconds and the system is now live.",
    }


@router.patch("/systems/{system_id}")
def update_system(
    system_id: str,
    payload: VMSSystemUpdate,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
) -> dict[str, Any]:
    """Edit name/vendor/department/ownership on an existing vms_systems row."""
    exists = db.execute(text("SELECT 1 FROM vms_systems WHERE id = :id"), {"id": system_id}).fetchone()
    if exists is None:
        raise HTTPException(status_code=404, detail="System not found")

    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        return {"id": system_id, "updated_fields": []}

    if updates.get("department_id") is not None:
        dept_row = db.execute(text(
            "SELECT 1 FROM departments WHERE id = :id"
        ), {"id": updates["department_id"]}).fetchone()
        if dept_row is None:
            raise HTTPException(
                status_code=400,
                detail=f"department_id {updates['department_id']!r} does not exist",
            )

    set_clause = ", ".join(f"{col} = :{col}" for col in updates)
    params = dict(updates)
    params["id"] = system_id
    db.execute(text(f"UPDATE vms_systems SET {set_clause} WHERE id = :id"), params)
    db.commit()

    return {"id": system_id, "updated_fields": list(updates.keys()), **updates}


@router.delete("/systems/{system_id}")
def delete_system(
    system_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
) -> dict[str, str]:
    """
    Delete a manually-onboarded (or adapter-registered) vms_systems row —
    refused with 409 while it still has cameras, rather than silently
    orphaning them: cameras.vms_system_id is ON DELETE SET NULL, so
    without this check a delete here would quietly turn federated
    cameras into what looks like main-grid ones instead of failing loudly.
    """
    exists = db.execute(text("SELECT 1 FROM vms_systems WHERE id = :id"), {"id": system_id}).fetchone()
    if exists is None:
        raise HTTPException(status_code=404, detail="System not found")

    camera_count = db.execute(text(
        "SELECT count(*) FROM cameras WHERE vms_system_id = :id"
    ), {"id": system_id}).scalar()
    if camera_count:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Cannot delete: {camera_count} camera(s) still reference this "
                "system. Reassign or remove them first."
            ),
        )

    db.execute(text("DELETE FROM vms_systems WHERE id = :id"), {"id": system_id})
    db.commit()
    return {"status": "deleted", "id": system_id}


@router.get("/cameras")
def get_federated_cameras(
    system_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Federated cameras (cameras with a non-null vms_system_id), optionally filtered by system_id."""
    q = """
        SELECT c.id, c.vms_system_id, c.source_grid_id, c.name,
               c.location_label, c.is_active,
               ST_Y(c.location::geometry) AS lat,
               ST_X(c.location::geometry) AS lng,
               vs.name AS system_name, vs.vendor
        FROM   cameras c
        JOIN   vms_systems vs ON vs.id = c.vms_system_id
    """
    params: dict = {}
    if system_id:
        q += " WHERE c.vms_system_id = :sys"
        params["sys"] = system_id
    q += " ORDER BY vs.name, c.name"

    rows = db.execute(text(q), params).fetchall()
    return [
        {
            "id":             str(r[0]),
            "system_id":      str(r[1]),
            "external_id":    r[2],
            "name":           r[3],
            "location_label": r[4],
            "is_active":      r[5],
            "lat":            float(r[6]) if r[6] is not None else None,
            "lng":            float(r[7]) if r[7] is not None else None,
            "system_name":    r[8],
            "vendor":         r[9],
        }
        for r in rows
    ]


@router.get("/events")
def get_federated_events(
    system_id: Optional[str] = Query(None),
    plate: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Recent federated detections (cameras with a non-null vms_system_id). Filterable by system_id and plate."""
    q = """
        SELECT d.id, c.vms_system_id, d.event_type, d.detected_plate,
               d.confidence, d.vehicle_type, d."timestamp", d.source_timestamp,
               vs.name AS system_name, vs.vendor,
               c.name AS camera_name
        FROM   detections d
        JOIN   cameras c ON c.id = d.camera_id
        JOIN   vms_systems vs ON vs.id = c.vms_system_id
        WHERE  c.vms_system_id IS NOT NULL
    """
    params: dict = {}
    if system_id:
        q += " AND c.vms_system_id = :sys"
        params["sys"] = system_id
    if plate:
        from model3_federation.correlation.engine import _normalize_plate
        params["plate"] = _normalize_plate(plate)
        q += " AND d.detected_plate = :plate"
    q += " ORDER BY d.\"timestamp\" DESC LIMIT :lim"
    params["lim"] = limit

    rows = db.execute(text(q), params).fetchall()
    return [
        {
            "id":               str(r[0]),
            "system_id":        str(r[1]),
            "event_type":       r[2],
            "detected_plate":   r[3],
            "confidence":       r[4],
            "vehicle_type":     r[5],
            "received_at":      r[6].isoformat() if r[6] else None,
            "source_timestamp": r[7].isoformat() if r[7] else None,
            "system_name":      r[8],
            "vendor":           r[9],
            "camera_name":      r[10],
        }
        for r in rows
    ]


@router.get("/events/stats")
def get_events_stats(
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Events per minute per system (live rate from in-memory counter)."""
    rows = db.execute(text(
        "SELECT id, name, vendor FROM vms_systems ORDER BY name"
    )).fetchall()

    return [
        {
            "system_id":      str(r[0]),
            "system_name":    r[1],
            "vendor":         r[2],
            "events_per_min": len(_events_per_min.get(str(r[0]), [])),
        }
        for r in rows
    ]


@router.get("/correlations")
def get_correlations(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """
    Cross-system correlations, most recent first. Computed on the fly from
    detections + vehicle_tracks — there's no stored correlation_results
    table to drift out of sync with the raw sightings (see engine.py's
    module docstring). A "correlation" here is any vehicle_track whose
    detections span more than one distinct vms_system_id.
    """
    rows = db.execute(text(
        """
        SELECT vt.id, vt.plate_number, vt.first_seen, vt.last_seen, vt.is_watchlisted,
               array_agg(DISTINCT vs.name) FILTER (WHERE vs.name IS NOT NULL) AS systems,
               count(DISTINCT c.vms_system_id) AS system_count
        FROM   vehicle_tracks vt
        JOIN   detections d ON d.vehicle_track_id = vt.id
        JOIN   cameras c ON c.id = d.camera_id
        LEFT JOIN vms_systems vs ON vs.id = c.vms_system_id
        WHERE  c.vms_system_id IS NOT NULL
        GROUP  BY vt.id
        HAVING count(DISTINCT c.vms_system_id) > 1
        ORDER  BY vt.last_seen DESC
        LIMIT  :lim
        """
    ), {"lim": limit}).fetchall()

    result = []
    for r in rows:
        travel_secs = int((r[3] - r[2]).total_seconds()) if r[2] and r[3] else None
        result.append({
            "id":               str(r[0]),
            "plate_number":     r[1],
            "systems_involved": r[5] or [],
            "first_seen":       r[2].isoformat() if r[2] else None,
            "last_seen":        r[3].isoformat() if r[3] else None,
            "travel_time_secs": travel_secs,
            "is_watchlisted":   r[4],
        })
    return result


@router.get("/correlations/track")
def track_vehicle(
    plate: str = Query(..., description="Vehicle plate number to track across all VMS systems"),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Full multi-system route for a specific plate number. Now that cameras
    live in one shared table, this naturally covers sightings from both
    the main grid and federated VMS systems, not just federated ones.
    """
    from model3_federation.correlation.engine import _normalize_plate
    plate_norm = _normalize_plate(plate)
    if not plate_norm:
        raise HTTPException(status_code=400, detail="plate parameter is required")

    rows = db.execute(text(
        """
        SELECT d.id, c.vms_system_id, d."timestamp", d.source_timestamp,
               d.confidence, d.vehicle_type,
               c.name AS camera_name, c.location_label,
               ST_Y(c.location::geometry) AS lat,
               ST_X(c.location::geometry) AS lng,
               vs.name AS system_name, vs.vendor
        FROM   detections d
        JOIN   cameras c ON c.id = d.camera_id
        LEFT JOIN vms_systems vs ON vs.id = c.vms_system_id
        WHERE  d.detected_plate = :p
        ORDER  BY d."timestamp" ASC
        LIMIT  100
        """
    ), {"p": plate_norm}).fetchall()

    sightings = [
        {
            "event_id":       str(r[0]),
            "system_id":      str(r[1]) if r[1] else None,
            "received_at":    r[2].isoformat() if r[2] else None,
            "source_timestamp": r[3].isoformat() if r[3] else None,
            "confidence":     r[4],
            "vehicle_type":   r[5],
            "camera_name":    r[6],
            "location_label": r[7],
            "lat":            float(r[8]) if r[8] is not None else None,
            "lng":            float(r[9]) if r[9] is not None else None,
            "system_name":    r[10] or "Sentinel Camera Grid",
            "vendor":         r[11],
        }
        for r in rows
    ]

    return {
        "plate":     plate_norm,
        "sightings": sightings,
        "count":     len(sightings),
    }


@router.get("/alerts")
def get_federated_alerts(
    limit: int = Query(30, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> list[dict[str, Any]]:
    """Federation-originated watchlist alerts (from cameras with a non-null vms_system_id), most recent first."""
    rows = db.execute(text(
        """
        SELECT a.id, a.created_at, a.severity, a.alert_type,
               a.acknowledged_at,
               d.detected_plate,
               c.name AS camera_name,
               vs.name AS system_name
        FROM   alerts a
        JOIN   detections d ON d.id = a.detection_id
        JOIN   cameras c ON c.id = d.camera_id
        JOIN   vms_systems vs ON vs.id = c.vms_system_id
        ORDER  BY a.created_at DESC
        LIMIT  :lim
        """
    ), {"lim": limit}).fetchall()

    return [
        {
            "id":             str(r[0]),
            "created_at":     r[1].isoformat() if r[1] else None,
            "severity":       r[2],
            "alert_type":     r[3],
            "acknowledged":   r[4] is not None,
            "plate_number":   r[5],
            "camera_name":    r[6],
            "system_name":    r[7],
        }
        for r in rows
    ]


@router.post("/alerts/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(require_role("dept_admin", "operator")),
) -> dict[str, str]:
    """Mark a federated alert as acknowledged. Same role gate as model2's write endpoints."""
    result = db.execute(text(
        """
        UPDATE alerts
        SET    acknowledged_at = now(),
               acknowledged_by = :uid
        WHERE  id = :id
          AND  acknowledged_at IS NULL
        RETURNING id
        """
    ), {"id": alert_id, "uid": str(current_user.id)}).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Alert not found or already acknowledged")

    db.commit()
    return {"status": "acknowledged", "alert_id": alert_id}


@router.post("/systems/{system_id}/simulate")
async def simulate_burst(
    system_id: str,
    db: Session = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Trigger a burst of 10 events from the specified VMS system.
    Used for live demo — gives judges an immediate flood of events to watch.
    """
    if _bus is None:
        raise HTTPException(status_code=503, detail="Federation bus not initialised")

    # Find the adapter for this system_id
    adapter = next(
        (a for a in _adapters if a.system_id == system_id),
        None,
    )
    if adapter is None:
        raise HTTPException(status_code=404, detail=f"No adapter found for system_id={system_id}")

    cameras = await adapter.get_cameras()
    if not cameras:
        raise HTTPException(status_code=404, detail="Adapter returned no cameras")

    import random
    plates = ["GJ05AB1234", "GJ01XX9999", "GJ04ZZ3210", "GJ01CD5678", "GJ03KL5566"]
    now = datetime.now(tz=timezone.utc)

    fired = 0
    for i in range(10):
        cam = random.choice(cameras)
        plate = plates[i % len(plates)]
        event = FederatedEvent(
            system_id=adapter.system_id,
            system_name=adapter.system_name,
            vendor=adapter.vendor,
            camera_external_id=cam.external_id,
            camera_name=cam.name,
            event_type="vehicle_detection",
            detected_plate=plate,
            confidence=round(random.uniform(0.78, 0.99), 2),
            vehicle_type=random.choice(["car", "truck", "motorcycle"]),
            source_timestamp=now,
            raw_payload={"simulated": True, "burst_index": i},
        )
        await _bus.publish(event)
        fired += 1
        await asyncio.sleep(0.05)  # Small delay so WS clients receive them as a stream

    return {"status": "ok", "events_fired": fired, "system": adapter.system_name}
