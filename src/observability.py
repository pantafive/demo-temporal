"""Temporal observability helpers.

- Search attribute keys used everywhere in this demo
- `upsert_phase` — one-liner to update CrmPhase + memo at phase boundaries
- `instrumented_activity` — slim decorator: start/ok/error logs only
  (no heartbeat; mock activities run in ms-seconds, heartbeats add noise)
"""

from __future__ import annotations

import functools
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import structlog
from temporalio import activity, workflow
from temporalio.common import SearchAttributeKey

CRM_USER_ID = SearchAttributeKey.for_keyword("CrmUserId")
CRM_NICKNAME = SearchAttributeKey.for_text("CrmNickname")
CRM_PHASE = SearchAttributeKey.for_keyword("CrmPhase")
CRM_FAILURE_RATE_SEED = SearchAttributeKey.for_float("CrmFailureRateSeed")
CRM_COMPENSATED = SearchAttributeKey.for_bool("CrmCompensated")


F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def instrumented_activity(phase: str) -> Callable[[F], F]:
    """Bind structlog context and log activity lifecycle. Apply under @activity.defn."""

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            info = activity.info()
            log = structlog.get_logger(__name__).bind(
                activity=info.activity_type,
                attempt=info.attempt,
                workflow_id=info.workflow_id,
                phase=phase,
            )
            t0 = time.monotonic()
            log.info(f"Activity {info.activity_type} started (attempt {info.attempt}).")
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                log.error(f"Activity {info.activity_type} failed: {exc!r}")
                raise
            log.info(
                f"Activity {info.activity_type} finished in "
                f"{int((time.monotonic() - t0) * 1000)}ms."
            )
            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def upsert_phase(phase: str, summary: str | None = None, **extra_memo: Any) -> None:
    """Update CrmPhase search attribute + memo at a workflow phase boundary."""
    workflow.upsert_search_attributes([CRM_PHASE.value_set(phase)])
    memo: dict[str, Any] = {"current_phase": phase, **extra_memo}
    if summary:
        memo["summary"] = summary
    workflow.upsert_memo(memo)
    workflow.logger.info(summary or f"Workflow phase: {phase}.", extra={"phase": phase})
