"""Worker entrypoint.

Runs as three replicas in docker-compose (worker-1/2/3), same image, distinct
WORKER_IDENTITY. All three poll the `crm-tasks` queue — visible as separate
pollers in the Temporal UI Workers tab. Whichever replica wins the bootstrap
race creates search attributes + the schedule; the others log-and-skip.
"""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import timedelta

import asyncpg
import structlog
from temporalio.worker import Worker

from activities import Activities
from common import (
    bootstrap_schedule,
    bootstrap_search_attributes,
    configure_logging,
    connect_client,
)
from gateways import (
    AnalyticsGateway,
    BillingGateway,
    EmailGateway,
    SearchGateway,
    SupportGateway,
)
from repo import UserRepository
from usecases import AuditUseCases, OnboardingUseCases
from workflows import ALL_WORKFLOWS

log = structlog.get_logger(__name__)

TASK_QUEUE = "crm-tasks"


async def main() -> None:
    configure_logging(service="worker")

    identity = os.environ.get("WORKER_IDENTITY", "worker-unknown")
    temporal_address = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/crm"
    )
    log.info("worker.starting", identity=identity, temporal_address=temporal_address)

    client = await connect_client(temporal_address)
    await bootstrap_search_attributes(client)
    await bootstrap_schedule(client, TASK_QUEUE)

    pool = await asyncpg.create_pool(database_url)
    assert pool is not None

    repo = UserRepository(pool)
    billing, analytics, search = BillingGateway(), AnalyticsGateway(), SearchGateway()
    email, support = EmailGateway(), SupportGateway()

    # Business logic lives here, framework-free. Temporal activities are
    # a thin wrapper; a REST handler could construct the same use-cases
    # and call them directly.
    onboarding = OnboardingUseCases(repo, billing, analytics, search, email, support)
    audit = AuditUseCases(repo, billing, analytics, search)

    activities = Activities(onboarding=onboarding, audit=audit)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=ALL_WORKFLOWS,
        activities=activities.all(),
        identity=identity,
        graceful_shutdown_timeout=timedelta(seconds=30),
    )

    log.info("worker.started", identity=identity, task_queue=TASK_QUEUE)
    async with worker:
        await stop_event.wait()

    await pool.close()
    log.info("worker.shutdown.complete", identity=identity)


if __name__ == "__main__":
    asyncio.run(main())
