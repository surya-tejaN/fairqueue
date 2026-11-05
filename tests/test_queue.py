from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.auth import issue_token
from app import main
from app.store import SqliteStore


@pytest.fixture(autouse=True)
def fresh_store():
    main.store = SqliteStore()
    yield


def client() -> TestClient:
    return TestClient(main.app)


def auth() -> dict:
    return {"Authorization": f"Bearer {issue_token('surya')}"}


def test_enqueue_requires_jwt():
    assert client().post("/jobs", json={"payload": {"n": 1}}).status_code == 401


def test_idempotent_enqueue_returns_same_job():
    headers = {**auth(), "Idempotency-Key": "same-once"}
    first = client().post("/jobs", json={"payload": {"n": 1}}, headers=headers)
    second = client().post("/jobs", json={"payload": {"n": 99}}, headers=headers)
    assert first.json()["id"] == second.json()["id"]
    assert second.json()["payload"] == {"n": 1}


def test_two_claims_cannot_take_the_same_job():
    store = SqliteStore()
    store.enqueue({"n": 1}, 3, None)
    first = store.claim(timedelta(seconds=30))
    second = store.claim(timedelta(seconds=30))
    assert first is not None and first.status == "running"
    assert second is None


def test_expired_running_job_can_be_reclaimed():
    store = SqliteStore()
    store.enqueue({"n": 1}, 3, None)
    first = store.claim(timedelta(seconds=0))
    again = store.claim(timedelta(seconds=30))
    assert first is not None and again is not None
    assert again.id == first.id
    assert again.attempts == 2


def test_complete_and_lookup():
    headers = auth()
    created = client().post("/jobs", json={"payload": {"n": 7}}, headers=headers).json()
    claimed = client().post("/internal/claim").json()
    assert claimed["id"] == created["id"]
    assert client().post(
        f"/internal/complete/{created['id']}", json={"result": {"ok": True}}
    ).status_code == 200
    got = client().get(f"/jobs/{created['id']}", headers=headers).json()
    assert got["status"] == "done"
    assert got["result"] == {"ok": True}
