import logging
import secrets
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from core.job_queue import BusyError, JobQueue
from core.quota import QuotaExceededError

logger = logging.getLogger("ubitokeer")

app = FastAPI(title="UbiTokeer", docs_url=None, redoc_url=None)

_queue: Optional[JobQueue] = None
_api_key: str = ""

# Care packages are downloaded straight from a Discord ticket by ordinary users,
# so this prefix is the ONE route that must stay unauthenticated.
PUBLIC_PATH_PREFIXES = ("/carepackage/",)
CAREPACKAGES_DIR = Path(__file__).parent.parent / "carepackages"


def set_queue(queue: JobQueue) -> None:
    global _queue
    _queue = queue


def set_api_key(key: str) -> None:
    global _api_key
    _api_key = (key or "").strip()
    if _api_key:
        logger.info("API key auth ENABLED — requests must send X-API-Key")
    else:
        logger.warning(
            "API key auth DISABLED (no api_key in config.json) — every endpoint on "
            "this port is open to anyone who can reach it. Set api_key to the same "
            "value the bot uses."
        )


@app.middleware("http")
async def require_api_key(request: Request, call_next):
    """Gate every endpoint except the public care-package downloads.

    This server binds 0.0.0.0 and, once a care-package link puts its address in
    front of users, anyone could otherwise POST /request to drain the token pool,
    read account emails from /accounts/health, or cancel other people's jobs. The
    bot already sends X-API-Key on its calls, so this is a drop-in lock.
    """
    if _api_key and not request.url.path.startswith(PUBLIC_PATH_PREFIXES):
        supplied = request.headers.get("X-API-Key", "")
        # Constant-time compare so the key can't be recovered by timing.
        if not secrets.compare_digest(supplied, _api_key):
            logger.warning(
                f"Rejected unauthenticated {request.method} {request.url.path} "
                f"from {request.client.host if request.client else 'unknown'}"
            )
            return JSONResponse(status_code=401, content={"error": "unauthorized"})
    return await call_next(request)


@app.get("/carepackage/{filename}")
def get_carepackage(filename: str):
    """Public care-package download (the only unauthenticated route).

    Users click this straight out of a Discord ticket, so it must serve without a
    key — which also means it must never be talked into serving anything except a
    zip that we deliberately placed in carepackages/.
    """
    # No traversal, no nested paths, zips only.
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="bad filename")
    if not filename.lower().endswith(".zip"):
        raise HTTPException(status_code=404, detail="not found")
    base = CAREPACKAGES_DIR.resolve()
    path = (base / filename).resolve()
    # Belt-and-braces: the resolved path must still sit inside carepackages/.
    if base not in path.parents:
        raise HTTPException(status_code=400, detail="bad filename")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="not found")
    return FileResponse(path, media_type="application/zip", filename=filename)


class JobRequest(BaseModel):
    uplay_id: str
    # Optional so a pure reservation (defer=True) can be made before the user has
    # produced their token request file. A normal immediate generate still sends it.
    token_req: str = ""
    # When True, hold a quota slot but DON'T generate yet — the bot opens the
    # ticket first, then calls /job/{id}/activate once the token_req arrives.
    defer: bool = False


class ActivateRequest(BaseModel):
    token_req: str


class CancelRequest(BaseModel):
    reason: str = ""


@app.post("/request")
def submit_request(body: JobRequest):
    logger.info(f"API: POST /request uplay_id={body.uplay_id} defer={body.defer}")
    try:
        if body.defer:
            # Admission gate: reserve a slot only. 429 here means the pool is full,
            # and the bot refuses to even open the ticket.
            job = _queue.reserve(body.uplay_id)
        else:
            if not body.token_req:
                return JSONResponse(status_code=400, content={"error": "token_req required"})
            job = _queue.submit(body.uplay_id, body.token_req)
        return {"job_id": job.id, "status": job.status.value}
    except QuotaExceededError as e:
        logger.warning(f"API: Quota exceeded — {e}")
        return JSONResponse(status_code=429, content={"error": str(e)})
    except BusyError as e:
        logger.warning(f"API: Queue full — {e}")
        return JSONResponse(status_code=503, content={"error": str(e)})
    except ValueError as e:
        logger.warning(f"API: Bad request — {e}")
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        logger.error(f"API: Unexpected error — {e}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/job/{job_id}")
def get_job(job_id: str):
    job = _queue.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.post("/job/{job_id}/activate")
def activate_job(job_id: str, body: ActivateRequest):
    """Promote a deferred RESERVED job to QUEUED, handing over the token request."""
    logger.info(f"API: POST /job/{job_id}/activate")
    try:
        job = _queue.activate(job_id, body.token_req)
        return {"job_id": job.id, "status": job.status.value}
    except BusyError as e:
        return JSONResponse(status_code=503, content={"error": str(e)})
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
    except Exception as e:
        logger.error(f"API: activate error — {e}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.post("/job/{job_id}/cancel")
def cancel_job(job_id: str, body: CancelRequest | None = None):
    """Release a held reservation (ticket closed/abandoned before generation)."""
    reason = body.reason if body else ""
    logger.info(f"API: POST /job/{job_id}/cancel reason={reason}")
    released = _queue.cancel(job_id, reason)
    return {"job_id": job_id, "released": released}


@app.get("/quota")
def get_quota():
    return _queue.get_quota_summary()


@app.get("/quota/{uplay_id}")
def get_quota_app(uplay_id: str):
    return _queue.get_quota_simple(uplay_id)


@app.get("/status")
def get_status():
    state = _queue.get_state()
    current = state["current"]
    pending = state["pending"]
    reservations = _queue.get_reservations()
    return {
        "status": "busy" if current else "idle",
        "queue_size": state.get("pending_count", 1 if pending else 0),
        "current_job": current,
        "pending_job": pending,
        # Slots held by open tickets that haven't uploaded a token_req yet.
        "reservations_total": reservations["total"],
        "reservations_by_uplay": reservations["by_uplay"],
    }


@app.get("/reservations")
def get_reservations():
    """Just the live reservation snapshot: {total, by_uplay}."""
    return _queue.get_reservations()


@app.get("/accounts/health")
def get_login_health():
    """Per-account LoginStore.dat session health.

    `needs_login: true` means that account's stored session is dead and someone
    has to sign it in again — surfaced here BEFORE a user's ticket fails on it.
    """
    return _queue.get_login_health()


class RefreshLoginsRequest(BaseModel):
    # force=True re-authenticates every account, not just the stale ones.
    force: bool = False


@app.post("/accounts/refresh")
def refresh_logins(body: RefreshLoginsRequest | None = None):
    """Re-authenticate stored sessions now, so idle accounts don't go dead.

    Costs nothing: the CLI stops at the appId prompt and never sends a ticket
    request, so no token is minted and no quota is spent.
    """
    force = bool(body.force) if body else False
    return {"ok": True, **_queue.refresh_logins(force=force)}


class ReconcileRequest(BaseModel):
    # job_ids of every ticket the bot still has open. Anything held here that
    # isn't in this list has no ticket behind it and gets released.
    active_job_ids: list[str] = []
    grace_seconds: float = 60.0


@app.post("/reservations/reconcile")
def reconcile_reservations(body: ReconcileRequest):
    """Drop held slots that no longer map to an open ticket.

    The bot's ticket table is the source of truth; it posts the job_ids of its
    live tickets every couple of minutes and we release the rest. This reclaims
    holds leaked by a deleted thread or a cancel that never landed, so the held
    count reflects the tickets that are actually open.
    """
    result = _queue.reconcile_reservations(body.active_job_ids, body.grace_seconds)
    return {"ok": True, **result}
