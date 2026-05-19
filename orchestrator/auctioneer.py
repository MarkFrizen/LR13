"""Аукцион для выбора оптимального агента-исполнителя.

Механизм:
  1. Оркестратор создаёт inbox и публикует задачу в ``auction.tasks``.
  2. Все агенты-оптимизаторы получают задачу, вычисляют стоимость
     (``cost = base + complexity * factor``) и публикуют ставки в inbox.
  3. Аукционер собирает ставки в течение ``BID_TIMEOUT`` секунд.
  4. Выбирает агента с минимальной стоимостью.
  5. Отправляет задачу в ``tasks.process`` с указанием ``target_agent_id``.
  6. Возвращает результат выполнения задачи.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from nats_utils import PipelineError, send_and_wait

logger = logging.getLogger("orchestrator.auction")

AUCTION_SUBJECT = "auction.tasks"
TASKS_SUBJECT = "tasks.process"
BID_TIMEOUT = 1.0  # секунд — время сбора ставок
DEFAULT_COMPLEXITY = 5
REFUSE_COST = 999999.0  # cost >= этого — агент отказался (перегружен)


async def run_optimizer_auction(
    nc: NATS,
    task_id: str,
    payload: dict[str, Any] | None = None,
    complexity: int = DEFAULT_COMPLEXITY,
    *,
    bid_timeout: float = BID_TIMEOUT,
) -> dict[str, Any]:
    """Провести аукцион среди агентов-оптимизаторов.

    1. Публикует задачу в ``auction.tasks`` с reply-каналом = inbox.
    2. Собирает ставки в течение ``bid_timeout``.
    3. Выбирает победителя (мин. стоимость).
    4. Отправляет задачу победителю в ``tasks.process``.
    5. Ждёт результат от агента.

    Args:
        nc: Подключение к NATS.
        task_id: ID задачи.
        payload: Данные для оптимизации.
        complexity: Сложность задачи (влияет на стоимость).
        bid_timeout: Время ожидания ставок (сек).

    Returns:
        Результат оптимизации (ответ агента).

    Raises:
        PipelineError: если не получено ни одной ставки,
                       или агент вернул ошибку.
    """
    inbox = nc.new_inbox()
    auction_task_id = f"{task_id}-auction"

    # ──────────────────────────────────────────────────────────
    # 1. Подписка на inbox для сбора ставок
    # ──────────────────────────────────────────────────────────
    bid_sub = await nc.subscribe(inbox)
    await nc.flush()

    # ──────────────────────────────────────────────────────────
    # 2. Публикация аукционной задачи
    # ──────────────────────────────────────────────────────────
    auction_task = {
        "task_id": auction_task_id,
        "type": "optimizer",
        "complexity": complexity,
        "payload": payload or {},
    }
    logger.info(
        "аукцион: публикация task_id=%s complexity=%d",
        auction_task_id, complexity,
    )
    await nc.publish(AUCTION_SUBJECT, json.dumps(auction_task).encode(), reply=inbox)

    # ──────────────────────────────────────────────────────────
    # 3. Сбор ставок
    # ──────────────────────────────────────────────────────────
    all_bids: list[dict[str, Any]] = []
    deadline = asyncio.get_event_loop().time() + bid_timeout

    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            break
        try:
            msg = await bid_sub.next_msg(timeout=remaining)
            bid = json.loads(msg.data)
            cost = bid.get("cost", 0)
            agent = bid.get("agent_id", "?")
            if cost >= REFUSE_COST:
                logger.info("аукцион: отказ от %s (cost=%.0f, перегружен)", agent, cost)
            else:
                all_bids.append(bid)
                logger.debug("аукцион: ставка от %s cost=%.2f", agent, cost)
        except asyncio.TimeoutError:
            break

    await bid_sub.unsubscribe()

    # ──────────────────────────────────────────────────────────
    # 4. Выбор победителя
    # ──────────────────────────────────────────────────────────
    if not all_bids:
        raise PipelineError(
            f"аукцион: ни один агент не принял задачу (task_id={auction_task_id})"
        )

    winner = min(all_bids, key=lambda b: b.get("cost", float("inf")))
    winner_id = winner["agent_id"]
    logger.info(
        "аукцион: победитель %s cost=%.2f (всего ставок=%d, отказов=%d)",
        winner_id, winner["cost"], len(all_bids),
        len([b for b in all_bids if b.get("cost", 0) >= REFUSE_COST]),
    )

    # ──────────────────────────────────────────────────────────
    # 5. Отправка задачи победителю
    # ──────────────────────────────────────────────────────────
    exec_task = {
        "task_id": task_id,
        "type": "optimizer",
        "target_agent_id": winner_id,
        "complexity": complexity,
        "payload": payload or {},
    }

    logger.info("аукцион: отправка задачи победителю %s", winner_id)
    result = await send_and_wait(nc, exec_task)

    logger.info(
        "аукцион: задача выполнена агентом %s, статус=%s",
        result.get("agent_id", winner_id),
        result.get("status", "?"),
    )

    return result
