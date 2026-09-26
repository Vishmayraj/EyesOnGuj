"""
Sentinel - Model 1 Registry & GIS
FastAPI application entry point.

# Boots the app, mounts routers, configures templates and static files.
# Run with:  uvicorn app.main:app --reload

"""

import asyncio
import logging
import os
import queue
import sys

from contextlib import asynccontextmanager
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Make `shared` importable when running locally
current_dir = Path(__file__).resolve().parent
local_repo_root = current_dir.parent.parent
if (local_repo_root / "shared").exists() and str(local_repo_root) not in sys.path:
    sys.path.insert(0, str(local_repo_root))

# Make bare `pipeline.*` imports (used by model2_analytics routers/pipeline,
# e.g. recorded.py -> from pipeline.video_worker import PreRecordedVideoWorker)
# resolvable outside Docker. Inside Docker the PYTHONPATH env var already
# covers this (see infra/Dockerfile); locally/CI it doesn't, so without this
# the Model 2 router auto-discovery loop below silently skips every router
# file that does a bare `pipeline.*` import (caught by its try/except),
# and those endpoints 404 instead of enforcing auth.
_M2_LOCAL_DIR = local_repo_root / "model2_analytics"
if _M2_LOCAL_DIR.exists() and str(_M2_LOCAL_DIR) not in sys.path:
    sys.path.insert(0, str(_M2_LOCAL_DIR))

from app.auth.dependencies import get_current_user  # noqa: E402
from app.config import settings  # noqa: E402
from app.routers import audit, auth, cameras, departments, districts, gap_analysis, pages, streams  # noqa: E402
from model3_federation.api.router import (  # noqa: E402
    router as federation_router,
    start_federation_services,
    stop_federation_services,
)
from shared.db.models import User as UserModel  # noqa: E402
from model2_analytics.app.ingestion.supervisor import IngestionSupervisor  # noqa: E402
from model2_analytics.app.ingestion.catalogue import (  # noqa: E402
    CataloguePoller,
    upsert_cameras_to_db,
    register_stream_in_mediamtx,
)
from model2_analytics.app.routers import (  # noqa: E402
    alerts as m2_alerts,
    detections as m2_detections,
    face_detection as m2_face_detection,
    grid as m2_grid,
    persons_watchlist as m2_persons_watchlist,
    recorded as m2_recorded,
    watchlist as m2_watchlist,
)
from shared.db.session import init_engine  # noqa: E402

# anpr.py's _broadcast_alert() looks up the detections module via
# sys.modules["model2.routers.detections"] (the key the old auto-discovery
# loop registered it under).  Keep that alias so the WS broadcast still
# finds the *same* module instance that owns ACTIVE_WS / _loop.
sys.modules.setdefault("model2.routers.detections", m2_detections)


async def _sync_cameras(supervisor: IngestionSupervisor, cameras, mediamtx_api: str) -> None:
    """
    Upsert cameras to DB (real UUIDs), sync workers, then register new
    streams in MediaMTX.
    """
    loop = asyncio.get_running_loop()
    rows = await loop.run_in_executor(None, upsert_cameras_to_db, cameras)
    running_before = set(supervisor._workers.keys())
    supervisor.sync(rows)
    for row in rows:
        grid_id = row.get("source_grid_id") or str(row.get("id"))
        rtsp_url = row.get("rtsp_url")
        if grid_id not in running_before and row.get("is_live") and rtsp_url:
            asyncio.create_task(
                register_stream_in_mediamtx(
                    mediamtx_api=mediamtx_api,
                    stream_name=grid_id,
                    rtsp_source_url=rtsp_url,
                )
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_engine(settings.DATABASE_URL)

    # Wire VMS ingestion layer (Task 7)
    frame_queue = queue.Queue(maxsize=500)
    app.state.frame_queue = frame_queue  # exact attribute name, analytics pipeline reads this

    supervisor = IngestionSupervisor(output_queue=frame_queue, mediamtx_api=settings.MEDIAMTX_API)
    app.state.supervisor = supervisor

    poller = CataloguePoller(grid_host=settings.GRID_HOST)
    app.state.poller = poller

    disable_ingestion = settings.DISABLE_INGESTION

    # DISABLE_INGESTION only turns off RTSP/MediaMTX registration - the
    # catalogue poll itself still runs (and still writes to the DB via
    # _db_only_sync below) so the registry stays in sync even with
    # streaming off. That's fine in normal operation, but it's wrong
    # during automated tests: CataloguePoller.fetch() makes a real HTTPS
    # call to the live grid host on every app startup, retries with
    # exponential backoff (2s -> 30s) whenever that call fails or times
    # out - which it always does with no network access, e.g. in CI or
    # any sandboxed/offline environment - and its fallback path still
    # writes cam01..cam30 to the `cameras` table through its own DB
    # session, outside of and concurrently with whatever transaction a
    # test is using. Against the isolated-per-test SAVEPOINT setup in
    # tests/conftest.py, that write can block on a lock the test already
    # holds on the very same seeded rows - which looks exactly like
    # `pytest` hanging/freezing for no visible reason. DISABLE_CATALOGUE_POLL
    # (set by tests/conftest.py before any TestClient is created) skips
    # starting this task entirely; it's unset (poll runs normally) for
    # every real deployment, including docker-compose.
    disable_catalogue_poll = os.environ.get("DISABLE_CATALOGUE_POLL", "false").lower() == "true"

    poll_task = None
    if not disable_catalogue_poll:
        if disable_ingestion:
            async def _db_only_sync(cams):
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, upsert_cameras_to_db, cams)

            poll_task = asyncio.create_task(
                poller.poll_forever(callback=_db_only_sync)
            )
        else:
            poll_task = asyncio.create_task(
                poller.poll_forever(
                    callback=lambda cams: _sync_cameras(supervisor, cams, settings.MEDIAMTX_API)
                )
            )

    # ── Model 3 Federation Services ─────────────────────────
    # start_federation_services registers each adapter's cameras into the
    # DB and starts its event stream as its own background task, then
    # returns — it does not block startup waiting on them.
    #
    # That registration goes through db_session_factory directly (the
    # module-level _SessionLocal, bound to settings.DATABASE_URL) instead
    # of the get_db dependency, so it's invisible to the per-test
    # SAVEPOINT/rollback session tests/conftest.py wires up via a get_db
    # override - same class of problem as the catalogue poll above, just
    # via a different DB path. In practice, every TestClient startup
    # writes real vms_systems/cameras rows straight into whatever database
    # settings.DATABASE_URL points at (the dev "sentinel" DB by default,
    # per .env.example) outside any test's transaction - which errors
    # outright if that database hasn't had shared/db/schema.sql applied to
    # it (bootstrap_local_db.sh only creates the role/database and
    # extensions, not the schema), and leaves real rows behind even when
    # it hasn't errored. DISABLE_FEDERATION_STARTUP (set by
    # tests/conftest.py, same mechanism as DISABLE_CATALOGUE_POLL above)
    # skips this entirely during tests; unset (services run normally) for
    # every real deployment. model3_federation/tests exercises
    # register_adapter() and the correlation engine directly against the
    # test session instead, so no coverage is lost by skipping this here.
    disable_federation_startup = os.environ.get("DISABLE_FEDERATION_STARTUP", "false").lower() == "true"
    if not disable_federation_startup:
        from shared.db.session import _SessionLocal as _sl
        await start_federation_services(db_session_factory=_sl, redis_url=settings.REDIS_URL)

    yield

    if poll_task is not None:
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
    supervisor.stop_all()
    # Safe to call unconditionally even when disable_federation_startup
    # skipped the start above - stop_federation_services() only cancels
    # whatever is in _tasks, which stays [] if nothing was ever started.
    await stop_federation_services()


app = FastAPI(
    title="Sentinel - Registry & GIS",
    description="Model 1: Camera registry, GIS mapping, and department/district management for Gujarat's CCTV network.",
    version="0.1.0",
    lifespan=lifespan,
)

# BUG-015 fix: configure strict CORS to prevent cross-origin exploits
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.ALLOWED_ORIGINS.split(",")],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ── Templates & static ──────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
app.state.templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Model-2 detection-image directory (cropped vehicle/plate images from the
# detection pipeline). NOT a plain StaticFiles mount (AuditReport1.md
# finding 1.5) — a FastAPI Depends() can't be attached directly to a
# StaticFiles mount, so this wraps the same directory in an explicit route
# that requires a logged-in user and rejects path traversal before ever
# touching the filesystem, instead of serving every file to anyone who can
_DETECTION_IMG_CANDIDATES = [
    Path("/model2-analytics/detection-image"),
    Path("/app/model2_analytics/detection-image"),
    local_repo_root / "model2_analytics" / "detection-image",
    local_repo_root / "model2-analytics" / "detection-image",
]
DETECTION_IMG_DIR = next((p for p in _DETECTION_IMG_CANDIDATES if p.is_dir()), _DETECTION_IMG_CANDIDATES[0])
DETECTION_IMG_DIR.mkdir(parents=True, exist_ok=True)
_DETECTION_IMG_DIR_RESOLVED = DETECTION_IMG_DIR.resolve()


@app.get("/detection-image/{file_path:path}", name="detection-image")
async def get_detection_image(
    file_path: str,
    current_user: UserModel = Depends(get_current_user),
):
    """Serve a detection-pipeline crop image, but only to a logged-in user."""
    requested = (_DETECTION_IMG_DIR_RESOLVED / file_path).resolve()
    try:
        requested.relative_to(_DETECTION_IMG_DIR_RESOLVED)
    except ValueError:
        # Path escapes DETECTION_IMG_DIR (e.g. "../../etc/passwd") — treat
        # exactly like "not found" rather than confirming it exists elsewhere.
        raise HTTPException(status_code=404, detail="Not found")
    if not requested.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(str(requested))

# ── Model 1 Routers ──────────────────────────────────────────────

app.include_router(auth.router)
app.include_router(audit.router)
app.include_router(cameras.router)
app.include_router(streams.router)
app.include_router(streams.streams_router)
app.include_router(departments.router)
app.include_router(districts.router)
app.include_router(gap_analysis.router)
app.include_router(pages.router)
app.include_router(federation_router)

# ── Model 2 Routers ──────────────────────────────────────────────

app.include_router(m2_alerts.router)
app.include_router(m2_detections.router)
app.include_router(m2_face_detection.router)
app.include_router(m2_grid.router)
app.include_router(m2_persons_watchlist.router)
app.include_router(m2_recorded.router)
app.include_router(m2_watchlist.router)
