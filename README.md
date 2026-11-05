# FairQueue

HTTP job queue. Workers claim a job, hold a visibility lease, and either complete it or let it become visible again. Failed jobs retry with backoff. `POST /jobs` requires a JWT. If the client sends `Idempotency-Key`, a retry returns the original job.

SQLite is the default store. Postgres is optional and claims with `FOR UPDATE SKIP LOCKED`.

## API

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/jobs` | JWT. Optional `Idempotency-Key`. Body: `{"payload": {}, "max_attempts": 3}` |
| GET | `/jobs/{id}` | JWT |
| POST | `/internal/claim` | Next available job |
| POST | `/internal/heartbeat/{id}` | Extends the lease |
| POST | `/internal/complete/{id}` | Body: `{"result": {}}` |
| POST | `/internal/fail/{id}` | Body: `{"error": "..."}` |

## Local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export FAIRQUEUE_JWT_SECRET=dev-secret
uvicorn app.main:app --reload
```

```bash
python -m app.worker
pytest
```

## Postgres

```bash
docker compose up -d
export FAIRQUEUE_STORE=postgres
export DATABASE_URL=postgresql://fairqueue:fairqueue@localhost:5432/fairqueue
```
