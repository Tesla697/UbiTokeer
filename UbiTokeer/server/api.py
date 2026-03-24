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
    token_req: str


@app.post("/request")
def submit_request(body: JobRequest):
    logger.info(f"API: POST /request uplay_id={body.uplay_id}")
    try:
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
    return {
        "status": "busy" if current else "idle",
        "queue_size": (1 if pending else 0),
        "current_job": current,
        "pending_job": pending,
    }
