"""Business logic — framework-free.

Each method is a single business action. None of this code imports Temporal,
FastAPI, or any transport. The same methods can be called from:

- a Temporal activity (see `activities.py`) — the production path for this demo,
- a REST handler — if you wanted synchronous execution without durability,
- a test — without spinning up a Temporal cluster.

The *orchestration* of these steps into a graph — what runs in parallel, what
retries, what compensates on failure — lives in `workflows.py`. That is a
transport concern, not a business concern.

Two groupings follow the bounded contexts of the demo:

- `OnboardingUseCases` — registering and deleting a user end-to-end.
- `AuditUseCases` — periodic read-side health checks.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import structlog

from contracts import CheckResult, GatewayResult, User
from gateways import (
    AnalyticsGateway,
    BillingGateway,
    EmailGateway,
    SearchGateway,
    SupportGateway,
)
from repo import UserRepository

log = structlog.get_logger(__name__)


class OnboardingUseCases:
    """Register / delete users. Forward actions + their compensations.

    Raises:
        DuplicateNicknameError: from `persist_user` when the nickname is
            already taken. Transport layer decides how to surface it.
    """

    def __init__(
        self,
        repo: UserRepository,
        billing: BillingGateway,
        analytics: AnalyticsGateway,
        search: SearchGateway,
        email: EmailGateway,
        support: SupportGateway,
    ) -> None:
        self.repo = repo
        self.billing = billing
        self.analytics = analytics
        self.search = search
        self.email = email
        self.support = support

    # --- Forward steps -----------------------------------------------------

    async def persist_user(self, user_id: UUID, nickname: str, workflow_id: str) -> User:
        user = User(
            id=user_id,
            nickname=nickname,
            created_at=datetime.now(UTC),
            last_workflow_id=workflow_id,
        )
        await self.repo.insert(user, workflow_id)
        return user

    async def create_billing_customer(self, user_id: UUID, nickname: str) -> GatewayResult:
        return await self.billing.create_customer(user_id, nickname)

    async def track_analytics_identify(self, user_id: UUID, nickname: str) -> GatewayResult:
        return await self.analytics.identify(user_id, nickname)

    async def index_user_in_search(self, user_id: UUID, nickname: str) -> GatewayResult:
        return await self.search.index_user(user_id, nickname)

    async def send_welcome_email(
        self, user_id: UUID, nickname: str, customer_id: str
    ) -> GatewayResult:
        return await self.email.send_welcome(user_id, nickname, customer_id)

    async def create_support_contact(
        self, user_id: UUID, nickname: str, customer_id: str, analytics_id: str
    ) -> GatewayResult:
        return await self.support.create_contact(user_id, nickname, customer_id, analytics_id)

    # --- Compensations -----------------------------------------------------

    async def delete_billing_customer(self, customer_id: str) -> None:
        await self.billing.delete_customer(customer_id)

    async def forget_analytics_user(self, analytics_id: str) -> None:
        await self.analytics.delete_identity(analytics_id)

    async def remove_from_search_index(self, doc_id: str) -> None:
        await self.search.delete_document(doc_id)

    async def note_email_apology(self, user_id: UUID, nickname: str) -> None:
        # Email cannot be unsent. Apology is the audit trail.
        log.info("compensation.email_apology_noted", user_id=str(user_id), nickname=nickname)

    async def delete_support_contact(self, contact_id: str) -> None:
        await self.support.delete_contact(contact_id)

    async def delete_user_record(self, user_id: UUID) -> None:
        await self.repo.delete(user_id)

    # --- Deletion helper ---------------------------------------------------

    async def lookup_user(self, user_id: UUID) -> User | None:
        return await self.repo.get(user_id)


class AuditUseCases:
    """Read-side health checks across external systems."""

    def __init__(
        self,
        repo: UserRepository,
        billing: BillingGateway,
        analytics: AnalyticsGateway,
        search: SearchGateway,
    ) -> None:
        self.repo = repo
        self.billing = billing
        self.analytics = analytics
        self.search = search

    async def list_all_users(self) -> list[User]:
        return await self.repo.list_all()

    async def check_user_health(self, user_id: UUID, nickname: str) -> CheckResult:
        billing_ok = await self._safe_probe(self.billing.probe, user_id)
        analytics_ok = await self._safe_probe(self.analytics.probe, user_id)
        search_ok = await self._safe_probe(self.search.probe, user_id)
        return CheckResult(
            user_id=user_id,
            nickname=nickname,
            billing_ok=billing_ok,
            analytics_ok=analytics_ok,
            search_ok=search_ok,
            healthy=billing_ok and analytics_ok and search_ok,
            duration_ms=0,
        )

    @staticmethod
    async def _safe_probe(fn, user_id: UUID) -> bool:
        """A failing probe means 'missing', not a workflow-level crash."""
        try:
            return bool(await fn(user_id))
        except Exception:
            return False
