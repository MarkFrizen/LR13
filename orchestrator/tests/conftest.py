"""Фикстуры и моки для тестирования pipeline."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

# ---------------------------------------------------------------------------
# Настройка путей импорта (тесты запускаются из корня проекта).
# ---------------------------------------------------------------------------
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Мок NATS
# ---------------------------------------------------------------------------
class MockNATS:
    """Полностью контролируемый мок NATS-клиента.

    Позволяет регистрировать ответы по типу задачи и
    ведёт лог всех вызовов ``request()``.

    Поддерживает:
    - request (request-reply) — возвращает зарегистрированный ответ
    - inbox-аукцион (publish + subscribe + new_inbox)
    """
    AUCTION_SUBJECT = "auction.tasks"

    def __init__(self) -> None:
        self._responses: dict[str, Any | None] = {}
        self.call_log: list[dict[str, Any]] = []
        self._inbox_count = 0
        # {inbox_subject: [dict, ...]} — сообщения для inbox-подписок
        self._inbox_msgs: dict[str, list[dict[str, Any]]] = {}
        # Предопределённые ставки для тестов аукциона
        self._auction_bids: list[dict[str, Any]] | None = None

    def set_auction_bids(self, bids: list[dict[str, Any]]) -> None:
        """Установить ставки, которые будут возвращены на аукционе."""
        self._auction_bids = bids

    def register(self, task_type: str, response: dict[str, Any] | None) -> None:
        """Зарегистрировать ответ для заданного типа задачи.

        ``None`` означает — симулировать таймаут (ошибка сети).
        """
        self._responses[task_type] = response

    # ---- Методы для поддержки inbox-аукциона ----

    def new_inbox(self) -> str:
        self._inbox_count += 1
        inbox = f"_INBOX.auction.{self._inbox_count:04d}"
        self._inbox_msgs[inbox] = []
        return inbox

    async def subscribe(self, subject: str, **kwargs) -> AsyncMock:
        sub = AsyncMock()
        sub.unsubscribe = AsyncMock()

        async def next_msg(*, timeout: float = 30.0) -> MockMsg:
            msgs = self._inbox_msgs.get(subject, [])
            if msgs:
                return MockMsg(msgs.pop(0))
            # Симулируем таймаут — ни один агент не ответил
            await asyncio.sleep(timeout)
            raise asyncio.TimeoutError()

        sub.next_msg = AsyncMock(side_effect=next_msg)
        return sub

    async def publish(self, subject: str, data: bytes, reply: str = "") -> None:
        """Эмулировать ``nats.client.publish()`` с поддержкой аукциона."""
        task = json.loads(data) if isinstance(data, bytes | str) else {}
        self.call_log.append({"$publish": {"subject": subject, "reply": reply}, **task})

        # Если это аукционная задача — генерируем фейковые ставки
        if reply and subject == self.AUCTION_SUBJECT:
            bids = self._auction_bids or [
                {"task_id": task.get("task_id", ""), "agent_id": "test-agent-001",
                 "cost": 8.5, "base_cost": 7.0, "complexity_factor": 1.2},
                {"task_id": task.get("task_id", ""), "agent_id": "test-agent-002",
                 "cost": 12.0, "base_cost": 10.0, "complexity_factor": 1.5},
            ]
            self._inbox_msgs.setdefault(reply, []).extend(bids)

    async def flush(self) -> None:
        pass

    # ---- Request-reply (существующий) ----

    async def request(
        self,
        subject: str,
        data: bytes,
        timeout: float = 30.0,
    ) -> MockMsg:
        """Эмулировать ``nats.client.request()``."""
        task = json.loads(data)
        self.call_log.append(task)

        task_type = task.get("type", "")
        response = self._responses.get(task_type, _UNKNOWN_TYPE)

        if response is None:
            import asyncio
            raise asyncio.TimeoutError(f"таймаут для task_type={task_type}")

        if response is _UNKNOWN_TYPE:
            raise RuntimeError(
                f"не зарегистрирован ответ для task_type='{task_type}'"
            )

        if callable(response):
            response = response(task)

        resp_copy = dict(response)
        if "task_id" not in resp_copy and "task_id" in task:
            resp_copy["task_id"] = task["task_id"]

        return MockMsg(resp_copy)

    @property
    def is_connected(self) -> bool:
        return True

    async def drain(self) -> None:
        pass

    async def close(self) -> None:
        pass


class MockMsg:
    """Эмуляция ``nats.aio.msg.Msg``."""

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = json.dumps(data).encode()


_UNKNOWN_TYPE = object()  # sentinel


# ---------------------------------------------------------------------------
# Тестовые данные
# ---------------------------------------------------------------------------
SAMPLE_CLIENTS = [
    {"id": 1, "name": "Alice", "age": 22, "region": "MSK", "purchases": 10},
    {"id": 2, "name": "Bob", "age": 35, "region": "SPB", "purchases": 5},
    {"id": 3, "name": "Charlie", "age": 28, "region": "MSK", "purchases": 3},
    {"id": 4, "name": "Diana", "age": 45, "region": "EKB", "purchases": 15},
    {"id": 5, "name": "Eve", "age": 19, "region": "SPB", "purchases": 0},
]

# ---------------------------------------------------------------------------
# Типовые ответы агентов
# ---------------------------------------------------------------------------

SEGMENT_RESPONSE: dict[str, Any] = {
    "task_id": "seg-stub",
    "type": "segment",
    "status": "completed",
    "segments": [
        {
            "name": "Возраст: 18-25",
            "description": "Молодёжь",
            "count": 3,
        },
        {
            "name": "Регион: MSK",
            "description": "Клиенты из региона MSK",
            "count": 2,
        },
    ],
}

SEGMENT_ERROR_RESPONSE: dict[str, Any] = {
    "task_id": "seg-stub",
    "type": "segment",
    "status": "error",
    "error": "временная недоступность БД",
}

SEGMENT_EMPTY_RESPONSE: dict[str, Any] = {
    "task_id": "seg-stub",
    "type": "segment",
    "status": "completed",
    "segments": [],
}


def campaign_response(task: dict[str, Any]) -> dict[str, Any]:
    """Сгенерировать ответ для кампании на основе переданного payload."""
    payload = task.get("payload", {})
    return {
        "task_id": task.get("task_id", "cmp-stub"),
        "type": "campaign",
        "status": "completed",
        "sent": 120,
        "failed": 2,
        "channel": payload.get("channel", "email"),
    }


HIGH_ROI_ANALYTICS: dict[str, Any] = {
    "task_id": "anl-stub",
    "type": "analytics",
    "status": "completed",
    "ctr": 8.5,
    "roi": 12.3,
    "opens": 480,
    "clicks": 95,
    "conversions": 15,
}

LOW_ROI_ANALYTICS: dict[str, Any] = {
    "task_id": "anl-stub",
    "type": "analytics",
    "status": "completed",
    "ctr": 0.3,
    "roi": 0.05,
    "opens": 25,
    "clicks": 1,
    "conversions": 0,
}

OPTIMIZER_RESPONSE: dict[str, Any] = {
    "task_id": "opt-stub",
    "type": "optimizer",
    "status": "completed",
    "old_budget": 50000.0,
    "new_budget": 57500.0,
    "change_percent": 15.0,
    "adjustment": "increase",
}


# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_nats() -> MockNATS:
    """Создать чистый MockNATS с пустым call_log."""
    return MockNATS()


@pytest.fixture
def save_state_mock() -> AsyncMock:
    """Мок для колбэка сохранения состояния."""
    return AsyncMock()
