"""Shared Pydantic contracts.

One file for every shape that crosses a process boundary — HTTP request,
workflow input, activity return, workflow result.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

NICKNAME_PATTERN = r"^[a-z0-9_]{3,32}$"


class DuplicateNicknameError(Exception):
    """Raised when a nickname already exists in the repository."""

    def __init__(self, nickname: str) -> None:
        self.nickname = nickname
        super().__init__(f"Nickname {nickname!r} is already taken")


class User(BaseModel):
    id: UUID
    nickname: str = Field(pattern=NICKNAME_PATTERN)
    created_at: datetime
    last_workflow_id: str | None = None


class RegisterUserCommand(BaseModel):
    user_id: UUID
    nickname: str = Field(pattern=NICKNAME_PATTERN)


class DeleteUserCommand(BaseModel):
    user_id: UUID


class GatewayResult(BaseModel):
    """Unified result from any external-system call.

    One shape across billing, analytics, search, email, support — the only
    thing that differs in reality is `id`, so we store it generically.
    Rich request/response snapshots land in ActivityTaskCompleted events.
    """

    id: str
    duration_ms: int
    attempts: int
    request: dict
    response: dict
    mock_delay_ms: int


class StepReport(BaseModel):
    name: str
    status: Literal["ok", "failed", "compensated"]
    duration_ms: int
    attempts: int
    error: str | None = None


class RegistrationResult(BaseModel):
    user_id: UUID
    nickname: str
    status: Literal["ok", "compensated", "failed"]
    started_at: datetime
    ended_at: datetime
    duration_ms: int
    forward_steps: list[StepReport]
    compensation_steps: list[StepReport]
    summary: str


class DeletionResult(BaseModel):
    user_id: UUID
    status: Literal["ok", "failed"]
    started_at: datetime
    ended_at: datetime
    duration_ms: int
    forward_steps: list[StepReport]
    summary: str


class CheckResult(BaseModel):
    user_id: UUID
    nickname: str
    billing_ok: bool
    analytics_ok: bool
    search_ok: bool
    healthy: bool
    duration_ms: int


class AuditReport(BaseModel):
    started_at: datetime
    ended_at: datetime
    duration_ms: int
    checked: int
    healthy: int
    unreachable: int
    per_user: list[CheckResult]
    summary: str
