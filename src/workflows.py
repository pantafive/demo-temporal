"""All Temporal workflows for the demo.

Two families:

1. Onboarding — `UserRegistrationWorkflow` (L1/L2 fan-out + saga) and
   `UserDeletionWorkflow` (parallel deletes + DB row).
2. Audit — `PeriodicUserAuditWorkflow` (schedule-driven parent) spawning
   one `CheckUserWorkflow` child per user.

Determinism: use `workflow.now()` and `workflow.random()` only. Never
import gateway or db modules at workflow scope — activities are invoked
by string name.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy

from contracts import (
    AuditReport,
    CheckResult,
    DeleteUserCommand,
    DeletionResult,
    GatewayResult,
    RegisterUserCommand,
    RegistrationResult,
    StepReport,
)
from observability import (
    CRM_COMPENSATED,
    CRM_FAILURE_RATE_SEED,
    CRM_NICKNAME,
    CRM_PHASE,
    CRM_USER_ID,
    upsert_phase,
)

_FORWARD = {
    "start_to_close_timeout": timedelta(seconds=30),
    "schedule_to_close_timeout": timedelta(minutes=2),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=10),
        maximum_attempts=3,
    ),
}

_COMPENSATE = {
    "start_to_close_timeout": timedelta(seconds=30),
    "retry_policy": RetryPolicy(
        initial_interval=timedelta(milliseconds=500),
        backoff_coefficient=2.0,
        maximum_interval=timedelta(seconds=5),
        maximum_attempts=10,
    ),
}


def _elapsed_ms(started_at) -> int:
    return int((workflow.now() - started_at).total_seconds() * 1000)


# ---------------------------------------------------------------------------
# Registration — L1 fan-out (3 parallel), L2 fan-out (2 parallel), saga on fail
# ---------------------------------------------------------------------------


@workflow.defn
class UserRegistrationWorkflow:
    @workflow.run
    async def run(self, cmd: RegisterUserCommand) -> RegistrationResult:
        started_at = workflow.now()
        uid = str(cmd.user_id)
        workflow.upsert_search_attributes(
            [
                CRM_USER_ID.value_set(uid),
                CRM_NICKNAME.value_set(cmd.nickname),
                CRM_PHASE.value_set("start"),
                CRM_FAILURE_RATE_SEED.value_set(workflow.random().random()),
                CRM_COMPENSATED.value_set(False),
            ]
        )
        workflow.upsert_memo(
            {
                "started_at": started_at.isoformat(),
                "user_id": uid,
                "nickname": cmd.nickname,
                "summary": (
                    f"Registering {cmd.nickname!r}: insert row, 3 parallel L1 calls, "
                    "2 dependent L2 calls, saga-compensate on any failure."
                ),
            }
        )

        forward: list[StepReport] = []
        completed: list[tuple[str, GatewayResult | None]] = []
        wf_id = workflow.info().workflow_id

        # Step 0 — persist_user. Failure here = no compensation.
        upsert_phase("persisting", summary=f"Inserting {cmd.nickname!r} into DB.")
        try:
            forward.append(
                await workflow.execute_activity(
                    "persist_user",
                    args=[uid, cmd.nickname, wf_id],
                    result_type=StepReport,
                    **_FORWARD,
                )
            )
            completed.append(("persist_user", None))
        except Exception as exc:
            return _failed_result(cmd, started_at, forward, [], f"Failed at persist_user: {exc}")

        # L1 — billing || analytics || search
        upsert_phase("fanning-out-l1", summary="Parallel L1: billing, analytics, search.")
        l1_specs = [
            ("create_billing_customer", [uid, cmd.nickname]),
            ("track_analytics_identify", [uid, cmd.nickname]),
            ("index_user_in_search", [uid, cmd.nickname]),
        ]
        l1 = await _gather(l1_specs, result_type=GatewayResult)
        billing, analytics, _search = _collect(l1_specs, l1, forward, completed)
        if any(isinstance(r, BaseException) for r in l1):
            await _saga_fail(cmd, completed, "L1", uid)

        # L2 — email (needs billing) || support (needs billing + analytics)
        assert billing is not None and analytics is not None
        upsert_phase("fanning-out-l2", summary="Parallel L2: email, support.")
        l2_specs = [
            ("send_welcome_email", [uid, cmd.nickname, billing.id]),
            ("create_support_contact", [uid, cmd.nickname, billing.id, analytics.id]),
        ]
        l2 = await _gather(l2_specs, result_type=GatewayResult)
        _collect(l2_specs, l2, forward, completed)
        if any(isinstance(r, BaseException) for r in l2):
            await _saga_fail(cmd, completed, "L2", uid)

        upsert_phase("complete", summary=f"All 6 activities for {cmd.nickname!r} succeeded.")
        return RegistrationResult(
            user_id=cmd.user_id,
            nickname=cmd.nickname,
            status="ok",
            started_at=started_at,
            ended_at=workflow.now(),
            duration_ms=_elapsed_ms(started_at),
            forward_steps=forward,
            compensation_steps=[],
            summary="All 6 steps completed successfully",
        )


async def _gather(specs: list[tuple[str, list]], *, result_type):
    return await asyncio.gather(
        *(
            workflow.execute_activity(name, args=args, result_type=result_type, **_FORWARD)
            for name, args in specs
        ),
        return_exceptions=True,
    )


def _collect(
    specs: list[tuple[str, list]],
    results: list,
    forward: list[StepReport],
    completed: list[tuple[str, GatewayResult | None]],
) -> list[GatewayResult | None]:
    """Append step reports + completed (name, payload) tuples. Return raw payloads."""
    out: list[GatewayResult | None] = []
    for (name, _), result in zip(specs, results, strict=True):
        if isinstance(result, BaseException):
            forward.append(
                StepReport(name=name, status="failed", duration_ms=0, attempts=1, error=str(result))
            )
            workflow.logger.error(f"Step {name!r} failed: {result!r}.")
            out.append(None)
        else:
            forward.append(StepReport(name=name, status="ok", duration_ms=0, attempts=1))
            completed.append((name, result))
            out.append(result)
    return out


# Mapping: forward-step name → (compensation activity, args builder from payload + context)
_COMPENSATIONS: dict = {
    "create_support_contact": (
        "delete_support_contact",
        lambda payload, uid, nickname: [payload.id],
    ),
    "send_welcome_email": (
        "email_apology_noted",
        lambda payload, uid, nickname: [uid, nickname],
    ),
    "index_user_in_search": (
        "remove_from_search_index",
        lambda payload, uid, nickname: [payload.id],
    ),
    "track_analytics_identify": (
        "forget_analytics_user",
        lambda payload, uid, nickname: [payload.id],
    ),
    "create_billing_customer": (
        "delete_billing_customer",
        lambda payload, uid, nickname: [payload.id],
    ),
    "persist_user": (
        "delete_user_record",
        lambda payload, uid, nickname: [uid],
    ),
}


async def _run_saga(
    completed: list[tuple[str, GatewayResult | None]], uid: str, nickname: str
) -> list[StepReport]:
    """Run compensations in reverse completion order."""
    upsert_phase(
        "compensating",
        summary=f"Rolling back {len(completed)} step(s) in reverse.",
    )
    steps: list[StepReport] = []
    for name, payload in reversed(completed):
        comp_name, build_args = _COMPENSATIONS[name]
        try:
            await workflow.execute_activity(
                comp_name, args=build_args(payload, uid, nickname), **_COMPENSATE
            )
            steps.append(
                StepReport(
                    name=f"compensate_{name}", status="compensated", duration_ms=0, attempts=1
                )
            )
        except Exception as exc:
            workflow.logger.error(f"Compensation for {name!r} failed: {exc!r}.")
            steps.append(
                StepReport(
                    name=f"compensate_{name}",
                    status="failed",
                    duration_ms=0,
                    attempts=1,
                    error=str(exc),
                )
            )
    return steps


async def _saga_fail(
    cmd: RegisterUserCommand,
    completed: list[tuple[str, GatewayResult | None]],
    stage: str,
    uid: str,
) -> None:
    """Run saga compensations and raise ApplicationError so Temporal marks Failed."""
    await _run_saga(completed, uid, cmd.nickname)
    workflow.upsert_search_attributes([CRM_COMPENSATED.value_set(True)])
    upsert_phase(
        "failed",
        summary=f"Registration failed at {stage}. Compensations ran in reverse.",
    )
    raise ApplicationError(f"registration_failed: {stage} failure", non_retryable=True)


def _failed_result(
    cmd: RegisterUserCommand,
    started_at,
    forward: list[StepReport],
    comp: list[StepReport],
    summary: str,
) -> RegistrationResult:
    upsert_phase("failed", summary=summary)
    return RegistrationResult(
        user_id=cmd.user_id,
        nickname=cmd.nickname,
        status="failed",
        started_at=started_at,
        ended_at=workflow.now(),
        duration_ms=_elapsed_ms(started_at),
        forward_steps=forward,
        compensation_steps=comp,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Deletion — parallel external deletes, then DB row
# ---------------------------------------------------------------------------


@workflow.defn
class UserDeletionWorkflow:
    @workflow.run
    async def run(self, cmd: DeleteUserCommand) -> DeletionResult:
        started_at = workflow.now()
        uid = str(cmd.user_id)
        workflow.upsert_search_attributes(
            [CRM_USER_ID.value_set(uid), CRM_PHASE.value_set("start")]
        )
        workflow.upsert_memo({"started_at": started_at.isoformat(), "user_id": uid})

        upsert_phase("lookup", summary=f"Looking up user {uid}.")
        user = await workflow.execute_activity(
            "lookup_user", args=[uid], result_type=dict, **_FORWARD
        )
        if user is None:
            upsert_phase("failed", summary="User not found.")
            return DeletionResult(
                user_id=cmd.user_id,
                status="failed",
                started_at=started_at,
                ended_at=workflow.now(),
                duration_ms=_elapsed_ms(started_at),
                forward_steps=[
                    StepReport(
                        name="lookup_user",
                        status="failed",
                        duration_ms=0,
                        attempts=1,
                        error="User not found",
                    )
                ],
                summary="User not found",
            )

        upsert_phase("del-fanout", summary="Parallel external deletes.")
        del_specs = [
            ("delete_billing_customer", [f"bill_{uid}"]),
            ("forget_analytics_user", [f"mix_{uid}"]),
            ("remove_from_search_index", [f"doc_{uid}"]),
            ("delete_support_contact", [f"sup_{uid}"]),
        ]
        results = await asyncio.gather(
            *(workflow.execute_activity(n, args=a, **_FORWARD) for n, a in del_specs),
            return_exceptions=True,
        )

        forward: list[StepReport] = []
        any_failed = False
        for (name, _), result in zip(del_specs, results, strict=True):
            if isinstance(result, BaseException):
                forward.append(
                    StepReport(
                        name=name, status="failed", duration_ms=0, attempts=1, error=str(result)
                    )
                )
                any_failed = True
            else:
                forward.append(StepReport(name=name, status="ok", duration_ms=0, attempts=1))

        upsert_phase("del-persist", summary="Removing user row from DB.")
        try:
            await workflow.execute_activity("delete_user_record", args=[uid], **_FORWARD)
            forward.append(
                StepReport(name="delete_user_record", status="ok", duration_ms=0, attempts=1)
            )
        except Exception as exc:
            forward.append(
                StepReport(
                    name="delete_user_record",
                    status="failed",
                    duration_ms=0,
                    attempts=1,
                    error=str(exc),
                )
            )
            any_failed = True

        status = "failed" if any_failed else "ok"
        upsert_phase(
            "failed" if any_failed else "complete",
            summary=(
                "Deletion finished with at least one failure."
                if any_failed
                else f"Deletion of {uid} complete."
            ),
        )
        return DeletionResult(
            user_id=cmd.user_id,
            status=status,  # type: ignore[arg-type]
            started_at=started_at,
            ended_at=workflow.now(),
            duration_ms=_elapsed_ms(started_at),
            forward_steps=forward,
            summary="Deletion complete" + (" with errors" if any_failed else ""),
        )


# ---------------------------------------------------------------------------
# Audit — schedule-driven parent, one child per user
# ---------------------------------------------------------------------------


@workflow.defn
class CheckUserWorkflow:
    """Run a single health check for one user. Spawned by the audit parent."""

    @workflow.run
    async def run(self, user_id: str, nickname: str = "") -> CheckResult:
        workflow.upsert_search_attributes([CRM_PHASE.value_set("checking")])
        return await workflow.execute_activity(
            "check_user",
            args=[user_id, nickname],
            result_type=CheckResult,
            **_FORWARD,
        )


@workflow.defn
class PeriodicUserAuditWorkflow:
    """Scheduled every minute. List users → spawn one child per user → aggregate."""

    @workflow.run
    async def run(self) -> AuditReport:
        started_at = workflow.now()
        run_id = workflow.info().run_id
        workflow.upsert_search_attributes([CRM_PHASE.value_set("listing")])

        upsert_phase("listing", summary="Listing all users.")
        users: list[dict] = await workflow.execute_activity(
            "list_all_users", result_type=list, **_FORWARD
        )
        total = len(users)

        upsert_phase("checking", summary=f"Spawning {total} child check(s).", total=total)
        handles = [
            await workflow.start_child_workflow(
                CheckUserWorkflow.run,
                args=[u["user_id"], u.get("nickname", "")],
                id=f"check-user-{u['user_id']}-{run_id}",
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
            for u in users
        ]
        results = await asyncio.gather(*handles, return_exceptions=True)

        per_user: list[CheckResult] = []
        unreachable = 0
        for r in results:
            if isinstance(r, BaseException):
                unreachable += 1
                workflow.logger.error(f"Child check failed: {r!r}.")
            else:
                per_user.append(r)  # type: ignore[arg-type]
                if not r.healthy:  # type: ignore[union-attr]
                    unreachable += 1

        healthy = sum(1 for r in per_user if r.healthy)
        summary = (
            f"Audit complete: {len(per_user)}/{total} users checked, "
            f"{healthy} healthy, {unreachable} unreachable"
        )
        upsert_phase("done", summary=summary, total=total)
        return AuditReport(
            started_at=started_at,
            ended_at=workflow.now(),
            duration_ms=_elapsed_ms(started_at),
            checked=len(per_user),
            healthy=healthy,
            unreachable=unreachable,
            per_user=per_user,
            summary=summary,
        )


ALL_WORKFLOWS = [
    UserRegistrationWorkflow,
    UserDeletionWorkflow,
    PeriodicUserAuditWorkflow,
    CheckUserWorkflow,
]
