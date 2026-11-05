from datetime import timedelta
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from app.auth import require_user
from app.models import EnqueueRequest, Job
from app.store import build_store

app = FastAPI(title="FairQueue")
store = build_store()
VISIBILITY = timedelta(seconds=30)


class ResultBody(BaseModel):
    result: dict


class FailBody(BaseModel):
    error: str


@app.post("/jobs", response_model=Job)
def enqueue(
    body: EnqueueRequest,
    user: str = Depends(require_user),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
) -> Job:
    _ = user
    return store.enqueue(body.payload, body.max_attempts, idempotency_key)


@app.get("/jobs/{job_id}", response_model=Job)
def get_job(job_id: str, user: str = Depends(require_user)) -> Job:
    _ = user
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.post("/internal/claim", response_model=Optional[Job])
def claim() -> Optional[Job]:
    return store.claim(VISIBILITY)


@app.post("/internal/heartbeat/{job_id}")
def heartbeat(job_id: str) -> dict:
    if not store.heartbeat(job_id, VISIBILITY):
        raise HTTPException(status_code=409, detail="job is not running")
    return {"ok": True}


@app.post("/internal/complete/{job_id}")
def complete(job_id: str, body: ResultBody) -> dict:
    if not store.complete(job_id, body.result):
        raise HTTPException(status_code=409, detail="job is not running")
    return {"ok": True}


@app.post("/internal/fail/{job_id}")
def fail(job_id: str, body: FailBody) -> dict:
    if not store.fail(job_id, body.error, timedelta(seconds=2)):
        raise HTTPException(status_code=404, detail="job not found")
    return {"ok": True}
