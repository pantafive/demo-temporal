"""Temporal activities — the Temporal↔business-logic boundary.

These are thin wrappers. Each activity:
  1. Converts primitives Temporal gave us (str) to domain types (UUID).
  2. Delegates to a `OnboardingUseCases` / `AuditUseCases` method.
  3. Translates domain errors into Temporal-shaped errors (e.g.
     `DuplicateNicknameError` → non-retryable `ApplicationError`).
  4. Builds a `StepReport` where the workflow wants one.

If you stripped out every `@activity.defn` line, the remaining code would be
uninteresting glue. That is the point — all business decisions live in
`usecases.py`.
"""

from __future__ import annotations

from uuid import UUID

from temporalio import activity
from temporalio.exceptions import ApplicationError

from contracts import CheckResult, DuplicateNicknameError, GatewayResult, StepReport
from observability import instrumented_activity
from usecases import AuditUseCases, OnboardingUseCases


class Activities:
    """Bound methods on this instance are registered with the Temporal worker."""

    def __init__(self, onboarding: OnboardingUseCases, audit: AuditUseCases) -> None:
        self.onb = onboarding
        self.audit = audit

    # --- Onboarding: forward -----------------------------------------------

    @activity.defn(name="persist_user")
    @instrumented_activity(phase="persist")
    async def persist_user(self, user_id: str, nickname: str, workflow_id: str) -> StepReport:
        try:
            await self.onb.persist_user(UUID(user_id), nickname, workflow_id)
        except DuplicateNicknameError as exc:
            raise ApplicationError(str(exc), type="DuplicateNickname", non_retryable=True) from exc
        return StepReport(
            name="persist_user", status="ok", duration_ms=0, attempts=activity.info().attempt
        )

    @activity.defn(name="create_billing_customer")
    @instrumented_activity(phase="billing")
    async def create_billing_customer(self, user_id: str, nickname: str) -> GatewayResult:
        return await self.onb.create_billing_customer(UUID(user_id), nickname)

    @activity.defn(name="track_analytics_identify")
    @instrumented_activity(phase="analytics")
    async def track_analytics_identify(self, user_id: str, nickname: str) -> GatewayResult:
        return await self.onb.track_analytics_identify(UUID(user_id), nickname)

    @activity.defn(name="index_user_in_search")
    @instrumented_activity(phase="search")
    async def index_user_in_search(self, user_id: str, nickname: str) -> GatewayResult:
        return await self.onb.index_user_in_search(UUID(user_id), nickname)

    @activity.defn(name="send_welcome_email")
    @instrumented_activity(phase="email")
    async def send_welcome_email(
        self, user_id: str, nickname: str, customer_id: str
    ) -> GatewayResult:
        return await self.onb.send_welcome_email(UUID(user_id), nickname, customer_id)

    @activity.defn(name="create_support_contact")
    @instrumented_activity(phase="support")
    async def create_support_contact(
        self, user_id: str, nickname: str, customer_id: str, analytics_id: str
    ) -> GatewayResult:
        return await self.onb.create_support_contact(
            UUID(user_id), nickname, customer_id, analytics_id
        )

    # --- Onboarding: compensation ------------------------------------------

    @activity.defn(name="delete_billing_customer")
    @instrumented_activity(phase="comp.billing")
    async def delete_billing_customer(self, customer_id: str) -> None:
        await self.onb.delete_billing_customer(customer_id)

    @activity.defn(name="forget_analytics_user")
    @instrumented_activity(phase="comp.analytics")
    async def forget_analytics_user(self, analytics_id: str) -> None:
        await self.onb.forget_analytics_user(analytics_id)

    @activity.defn(name="remove_from_search_index")
    @instrumented_activity(phase="comp.search")
    async def remove_from_search_index(self, doc_id: str) -> None:
        await self.onb.remove_from_search_index(doc_id)

    @activity.defn(name="email_apology_noted")
    @instrumented_activity(phase="comp.email")
    async def email_apology_noted(self, user_id: str, nickname: str) -> None:
        await self.onb.note_email_apology(UUID(user_id), nickname)

    @activity.defn(name="delete_support_contact")
    @instrumented_activity(phase="comp.support")
    async def delete_support_contact(self, contact_id: str) -> None:
        await self.onb.delete_support_contact(contact_id)

    @activity.defn(name="delete_user_record")
    @instrumented_activity(phase="comp.persist")
    async def delete_user_record(self, user_id: str) -> None:
        await self.onb.delete_user_record(UUID(user_id))

    # --- Deletion workflow -------------------------------------------------

    @activity.defn(name="lookup_user")
    @instrumented_activity(phase="del.lookup")
    async def lookup_user(self, user_id: str) -> dict | None:
        user = await self.onb.lookup_user(UUID(user_id))
        return {"user_id": str(user.id), "nickname": user.nickname} if user else None

    # --- Audit workflow ----------------------------------------------------

    @activity.defn(name="list_all_users")
    @instrumented_activity(phase="audit.list")
    async def list_all_users(self) -> list[dict]:
        users = await self.audit.list_all_users()
        return [{"user_id": str(u.id), "nickname": u.nickname} for u in users]

    @activity.defn(name="check_user")
    @instrumented_activity(phase="audit.check")
    async def check_user(self, user_id: str, nickname: str) -> CheckResult:
        return await self.audit.check_user_health(UUID(user_id), nickname)

    def all(self) -> list:
        """Every @activity.defn bound method on this instance, for Worker registration."""
        return [
            self.persist_user,
            self.create_billing_customer,
            self.track_analytics_identify,
            self.index_user_in_search,
            self.send_welcome_email,
            self.create_support_contact,
            self.delete_billing_customer,
            self.forget_analytics_user,
            self.remove_from_search_index,
            self.email_apology_noted,
            self.delete_support_contact,
            self.delete_user_record,
            self.lookup_user,
            self.list_all_users,
            self.check_user,
        ]
