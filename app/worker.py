import time
from datetime import timedelta

from app.store import build_store

store = build_store()


def handle(payload: dict) -> dict:
    return {"echo": payload}


def run() -> None:
    while True:
        job = store.claim(timedelta(seconds=30))
        if job is None:
            time.sleep(0.25)
            continue
        if job.status == "dead":
            continue
        try:
            store.complete(job.id, handle(job.payload))
        except Exception as exc:  # noqa: BLE001
            store.fail(job.id, str(exc), timedelta(seconds=2))


if __name__ == "__main__":
    run()
