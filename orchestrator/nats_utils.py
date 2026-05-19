"""Общие NATS-утилиты для pipeline и auctioneer."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from nats.aio.client import Client as NATS

logger = logging.getLogger("orchestrator.nats_utils")

REQUEST_TIMEOUT = 30.0
SUBJECT_TASKS = "tasks.process"


class PipelineError(Exception):
    """Ошибка выполнения pipeline."""


async def send_and_wait(
    nc: NATS,
    task: dict[str, Any],
    *,
    timeout: float = REQUEST_TIMEOUT,
) -> dict[str, Any]:
    """Отправить задачу в NATS и дождаться ответа (request-reply)."""
    task_id = task.get("task_id", "unknown")
    task_type = task.get("type", "unknown")
    logger.info("отправка задачи type=%s task_id=%s", task_type, task_id)

    try:
        msg = await nc.request(
            SUBJECT_TASKS,
            json.dumps(task).encode(),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        raise PipelineError(
            f"таймаут ожидания ответа от агента '{task_type}' "
            f"(task_id={task_id}, timeout={timeout}s)"
        )
    except Exception as exc:
        raise PipelineError(
            f"ошибка NATS при отправке задачи '{task_type}': {exc}"
        ) from exc

    try:
        result = json.loads(msg.data)
    except json.JSONDecodeError as exc:
        raise PipelineError(
            f"невалидный JSON в ответе агента '{task_type}': {exc}"
        ) from exc

    status = result.get("status")
    if status == "error":
        err_msg = result.get("error", "неизвестная ошибка")
        logger.error("агент '%s' вернул ошибку: %s", task_type, err_msg)
        raise PipelineError(f"агент '{task_type}': {err_msg}")

    logger.info(
        "получен ответ от агента '%s' task_id=%s status=%s",
        task_type, task_id, status,
    )
    return result
