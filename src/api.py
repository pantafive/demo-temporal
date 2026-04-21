"""FastAPI app — add/list/delete users.

Each write starts a Temporal workflow and waits for its result. HTMX fragments
are served directly from Jinja templates under `templates/`.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import jinja2
import structlog
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import ValidationError
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from common import configure_logging, connect_client
from contracts import DeleteUserCommand, DuplicateNicknameError, RegisterUserCommand
from repo import UserRepository
from workflows import UserDeletionWorkflow, UserRegistrationWorkflow

log = structlog.get_logger(__name__)

TASK_QUEUE = "crm-tasks"
NO_STORE = {"Cache-Control": "no-store"}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(service="api", level=os.environ.get("LOG_LEVEL", "INFO"))

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    assert pool is not None
    client = await connect_client(
        os.environ.get("TEMPORAL_HOST", "temporal:7233"),
        os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    templates = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(Path(__file__).parent / "templates")),
        autoescape=jinja2.select_autoescape(["html"]),
    )

    app.state.pool = pool
    app.state.client = client
    app.state.repo = UserRepository(pool)
    app.state.templates = templates
    log.info("api.startup.ready")

    yield

    await pool.close()
    log.info("api.shutdown.complete")


app = FastAPI(title="CRM Demo", version="1.0.0", lifespan=lifespan)


def _ui_url() -> str:
    return os.environ.get("TEMPORAL_UI_URL", "http://localhost:8080")


def _render(request: Request, name: str, **ctx) -> str:
    return request.app.state.templates.get_template(name).render(**ctx)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    users = await request.app.state.repo.list_all()
    return HTMLResponse(_render(request, "index.html", users=users, temporal_ui_url=_ui_url()))


@app.get("/api/users", response_class=HTMLResponse)
async def list_users(request: Request) -> HTMLResponse:
    users = await request.app.state.repo.list_all()
    rows = "".join(
        _render(request, "_user_row.html", u=u, temporal_ui_url=_ui_url()) for u in users
    )
    return HTMLResponse(rows, headers=NO_STORE)


@app.post("/api/users", response_class=HTMLResponse)
async def create_user(request: Request, nickname: str = Form(...)) -> HTMLResponse:
    user_id = uuid4()
    try:
        cmd = RegisterUserCommand(user_id=user_id, nickname=nickname.strip())
    except ValidationError:
        return _error_fragment(
            request,
            f"Invalid nickname '{nickname}': must be 3-32 chars of [a-z0-9_].",
            422,
        )
    client: Client = request.app.state.client
    log.info("api.create_user.start", user_id=str(user_id), nickname=nickname)

    try:
        handle = await client.start_workflow(
            UserRegistrationWorkflow.run,
            cmd,
            id=f"register-{user_id}",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        await handle.result()
    except DuplicateNicknameError:
        return _error_fragment(request, f"Nickname '{nickname}' is already taken.", 409)
    except Exception as exc:
        if "DuplicateNickname" in str(exc) or "duplicate" in str(exc).lower():
            return _error_fragment(request, f"Nickname '{nickname}' is already taken.", 409)
        log.error("api.create_user.error", nickname=nickname, error=str(exc))
        return _error_fragment(request, f"Registration failed: {exc}", 500)

    user = await request.app.state.repo.get(user_id)
    if user is None:
        return _error_fragment(request, "Registration finished but user not found.", 500)

    return HTMLResponse(
        _render(request, "_user_row.html", u=user, temporal_ui_url=_ui_url()),
        headers=NO_STORE,
    )


@app.delete("/api/users/{user_id}", response_class=HTMLResponse)
async def delete_user(user_id: UUID, request: Request) -> HTMLResponse:
    cmd = DeleteUserCommand(user_id=user_id)
    client: Client = request.app.state.client
    log.info("api.delete_user.start", user_id=str(user_id))

    try:
        handle = await client.start_workflow(
            UserDeletionWorkflow.run,
            cmd,
            id=f"delete-{user_id}",
            task_queue=TASK_QUEUE,
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
        await handle.result()
    except Exception as exc:
        log.error("api.delete_user.error", user_id=str(user_id), error=str(exc))

    workflow_url = f"{_ui_url()}/namespaces/default/workflows/delete-{user_id}"
    flash = (
        f'<div class="flash" id="flash-{user_id}">'
        f'Deletion started: <a href="{workflow_url}" target="_blank">delete-{user_id}</a>'
        f"</div>"
    )
    return HTMLResponse(flash, headers={"HX-Trigger": "user-deleted", "Cache-Control": "no-store"})


def _error_fragment(request: Request, msg: str, status: int) -> HTMLResponse:
    return HTMLResponse(
        _render(request, "_error.html", msg=msg), status_code=status, headers=NO_STORE
    )
