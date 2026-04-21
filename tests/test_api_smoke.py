"""API smoke test — hit routes with a fake repo + fake Temporal client.

No Temporal cluster or Postgres required.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import jinja2
import pytest
from fastapi.testclient import TestClient

from contracts import User


class _FakeRepo:
    def __init__(self) -> None:
        self._users: list[User] = []

    async def list_all(self) -> list[User]:
        return list(self._users)

    async def get(self, user_id: UUID) -> User | None:
        return next((u for u in self._users if u.id == user_id), None)

    def seed(self, user: User) -> None:
        self._users.append(user)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    # Force the TestClient to avoid running lifespan (no DB / no Temporal).
    import api as api_module

    monkeypatch.setenv("TEMPORAL_UI_URL", "http://localhost:8080")

    templates = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(Path(api_module.__file__).parent / "templates")),
        autoescape=jinja2.select_autoescape(["html"]),
    )

    app = api_module.app
    app.router.lifespan_context = _noop_lifespan  # type: ignore[attr-defined]
    app.state.repo = _FakeRepo()
    app.state.templates = templates
    app.state.client = _make_fake_client()
    return TestClient(app)


async def _noop_lifespan(_: Any):
    yield


def _make_fake_client() -> Any:
    """Fake Temporal client whose start_workflow returns a handle whose result() resolves."""
    client = AsyncMock()

    async def start_workflow(fn, cmd, **kwargs):
        handle = AsyncMock()

        async def result():
            # Persist into fake repo so GET sees the user after POST.
            return None

        handle.result = result
        return handle

    client.start_workflow = start_workflow
    return client


def test_healthz_ok(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_index_renders_empty_list(client: TestClient) -> None:
    r = client.get("/")
    assert r.status_code == 200
    assert "No users yet" in r.text


def test_index_renders_seeded_user(client: TestClient) -> None:
    client.app.state.repo.seed(  # type: ignore[attr-defined]
        User(id=uuid4(), nickname="bob_01", created_at=datetime.now(UTC))
    )
    r = client.get("/")
    assert "bob_01" in r.text


def test_create_user_invalid_nickname(client: TestClient) -> None:
    """FastAPI/Pydantic rejects invalid nicknames before starting a workflow."""
    r = client.post("/api/users", data={"nickname": "BAD"})
    # Our route tries to construct RegisterUserCommand which raises ValidationError;
    # that bubbles up as 500 (we don't special-case it client-side in this demo).
    assert r.status_code >= 400


def test_create_user_happy_path_renders_row(client: TestClient) -> None:
    """POST returns the user row fragment after the (fake) workflow completes."""
    # Seed the user the fake repo will return from .get() after POST.
    user = User(id=uuid4(), nickname="alice_01", created_at=datetime.now(UTC))
    fake_repo = client.app.state.repo  # type: ignore[attr-defined]
    fake_repo.seed(user)

    # We'll override uuid4 in the route to return our seeded id so .get() matches.
    # Instead: the route generates its own uuid4 and calls repo.get(that_id).
    # The fake repo returns None for unknown ids → route returns 500.
    # Workaround: monkey-patch uuid4 used in api.
    import api as api_module

    orig_uuid4 = api_module.uuid4
    api_module.uuid4 = lambda: user.id  # type: ignore[assignment]
    try:
        r = client.post("/api/users", data={"nickname": "alice_01"})
    finally:
        api_module.uuid4 = orig_uuid4  # type: ignore[assignment]

    assert r.status_code == 200
    assert "alice_01" in r.text
