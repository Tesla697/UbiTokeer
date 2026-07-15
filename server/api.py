import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.job_queue import BusyError, JobQueue
from core.quota import QuotaExceededError

logger = logging.getLogger("ubitokeer")

app = FastAPI(title="UbiTokeer", docs_url=None, redoc_url=None)

_queue: Optional[JobQueue] = None


def set_queue(queue: JobQueue) -> None:
    global _queue
    _queue = queue


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
