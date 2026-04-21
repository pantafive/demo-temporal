"""Mock implementations for the 5 external systems.

Every gateway goes through `flaky_call` which sleeps a random amount and
fails with probability `MOCK_FAILURE_RATE`. Tweak those envs to watch
Temporal retries and saga compensations stack up.
"""

import asyncio
import os
import random
import time

import structlog
from ulid import ULID

from contracts import GatewayResult

log = structlog.get_logger(__name__)


class FlakyExternalError(Exception):
    def __init__(self, gateway: str) -> None:
        super().__init__(f"Simulated transient failure from {gateway}")


def _latency_bounds() -> tuple[int, int]:
    demo = os.environ.get("DEMO_MODE", "false").lower() in ("1", "true", "yes")
    default_min, default_max = (3000, 6000) if demo else (300, 2000)
    return (
        int(os.environ.get("MOCK_MIN_MS", default_min)),
        int(os.environ.get("MOCK_MAX_MS", default_max)),
    )


async def flaky_call(name: str) -> int:
    """Sleep, then maybe raise. Returns actual delay_ms."""
    min_ms, max_ms = _latency_bounds()
    log.info("gateway.call.start", gateway=name)

    t0 = time.monotonic()
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000.0)
    delay_ms = int((time.monotonic() - t0) * 1000)

    failure_rate = float(os.environ.get("MOCK_FAILURE_RATE", "0.2"))
    if random.random() < failure_rate:
        log.warning("gateway.call.flaky_error", gateway=name, delay_ms=delay_ms)
        raise FlakyExternalError(name)

    log.info("gateway.call.ok", gateway=name, duration_ms=delay_ms)
    return delay_ms


async def _do_call(name: str, prefix: str, request: dict, extra_response: dict) -> GatewayResult:
    t0 = time.monotonic()
    delay_ms = await flaky_call(name)
    out_id = f"{prefix}_{ULID()}"
    return GatewayResult(
        id=out_id,
        duration_ms=int((time.monotonic() - t0) * 1000),
        attempts=1,
        request=request,
        response={"id": out_id, **extra_response},
        mock_delay_ms=delay_ms,
    )


class BillingGateway:
    async def create_customer(self, user_id: object, nickname: str) -> GatewayResult:
        return await _do_call(
            "billing.create_customer",
            "bill",
            {"user_id": str(user_id), "nickname": nickname},
            {"status": "active"},
        )

    async def delete_customer(self, customer_id: str) -> None:
        await flaky_call("billing.delete_customer")

    async def probe(self, user_id: object) -> bool:
        return True  # read-side probe always "found" for this mock


class AnalyticsGateway:
    async def identify(self, user_id: object, nickname: str) -> GatewayResult:
        return await _do_call(
            "analytics.identify",
            "mix",
            {"user_id": str(user_id), "nickname": nickname},
            {"traits_count": 2},
        )

    async def delete_identity(self, analytics_id: str) -> None:
        await flaky_call("analytics.delete_identity")

    async def probe(self, user_id: object) -> bool:
        return True


class SearchGateway:
    async def index_user(self, user_id: object, nickname: str) -> GatewayResult:
        return await _do_call(
            "search.index_user",
            "doc",
            {"user_id": str(user_id), "nickname": nickname},
            {"indexed": True},
        )

    async def delete_document(self, doc_id: str) -> None:
        await flaky_call("search.delete_document")

    async def probe(self, user_id: object) -> bool:
        return True


class EmailGateway:
    async def send_welcome(self, user_id: object, nickname: str, customer_id: str) -> GatewayResult:
        return await _do_call(
            "email.send_welcome",
            "msg",
            {"user_id": str(user_id), "nickname": nickname, "customer_id": customer_id},
            {"status": "queued"},
        )


class SupportGateway:
    async def create_contact(
        self, user_id: object, nickname: str, customer_id: str, analytics_id: str
    ) -> GatewayResult:
        return await _do_call(
            "support.create_contact",
            "sup",
            {
                "user_id": str(user_id),
                "nickname": nickname,
                "customer_id": customer_id,
                "analytics_id": analytics_id,
            },
            {"tier": "standard"},
        )

    async def delete_contact(self, contact_id: str) -> None:
        await flaky_call("support.delete_contact")
