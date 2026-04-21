"""Shared helpers: Temporal client, logging, cluster bootstrap."""

from __future__ import annotations

import logging
import sys
from datetime import timedelta

import structlog
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import RPCError, RPCStatusCode

log = structlog.get_logger(__name__)

_log_configured = False


def configure_logging(service: str, level: str = "INFO") -> None:
    """Idempotent structlog + stdlib logging setup with JSON output."""
    global _log_configured
    if _log_configured:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.contextvars.merge_contextvars,
            structlog.processors.format_exc_info,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service)
    _log_configured = True


async def connect_client(address: str, namespace: str = "default") -> Client:
    """Temporal client with pydantic_data_converter so our BaseModels round-trip."""
    return await Client.connect(
        address, namespace=namespace, data_converter=pydantic_data_converter
    )


_SEARCH_ATTRS = [
    ("CrmUserId", "KEYWORD"),
    ("CrmNickname", "TEXT"),
    ("CrmPhase", "KEYWORD"),
    ("CrmFailureRateSeed", "DOUBLE"),
    ("CrmCompensated", "BOOL"),
]


async def bootstrap_search_attributes(client: Client) -> None:
    """Register custom search attributes. Safe across multiple replicas."""
    from temporalio.api.enums.v1 import IndexedValueType
    from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest

    type_map = {
        "KEYWORD": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
        "TEXT": IndexedValueType.INDEXED_VALUE_TYPE_TEXT,
        "DOUBLE": IndexedValueType.INDEXED_VALUE_TYPE_DOUBLE,
        "BOOL": IndexedValueType.INDEXED_VALUE_TYPE_BOOL,
    }
    try:
        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(
                namespace=client.namespace,
                search_attributes={n: type_map[k] for n, k in _SEARCH_ATTRS},
            )
        )
        log.info("search_attributes.registered", count=len(_SEARCH_ATTRS))
    except RPCError as exc:
        if exc.status == RPCStatusCode.ALREADY_EXISTS:
            log.info("search_attributes.already_registered")
        else:
            raise


async def bootstrap_schedule(client: Client, task_queue: str) -> None:
    """Create the every-minute user-audit schedule. Safe across multiple replicas."""
    from workflows import PeriodicUserAuditWorkflow

    schedule_id = "user-audit"
    try:
        await client.create_schedule(
            schedule_id,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    PeriodicUserAuditWorkflow.run, id=schedule_id, task_queue=task_queue
                ),
                spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=1))]),
            ),
        )
        log.info("schedule.created", schedule_id=schedule_id)
    except RPCError as exc:
        if exc.status == RPCStatusCode.ALREADY_EXISTS:
            log.info("schedule.already_exists", schedule_id=schedule_id)
        else:
            raise
    except Exception as exc:
        # SDK may raise a different exception type for already-exists on some versions
        if "AlreadyExists" in type(exc).__name__ or "already" in str(exc).lower():
            log.info("schedule.already_exists", schedule_id=schedule_id)
        else:
            raise
