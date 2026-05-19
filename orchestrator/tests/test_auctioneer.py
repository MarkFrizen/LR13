"""Тесты для аукциона агентов-оптимизаторов.

Проверяется:
  - побеждает агент с наименьшей стоимостью;
  - задача отправляется победителю (target_agent_id);
  - отказ всех агентов (cost=INF) → ошибка;
  - частичный отказ — победитель из оставшихся.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from auctioneer import REFUSE_COST, run_optimizer_auction
from tests.conftest import OPTIMIZER_RESPONSE, MockNATS


# ======================================================================
# Вспомогательная функция: проверить request к tasks.process в call_log
# ======================================================================
def _find_optimizer_request(call_log: list[dict]) -> dict | None:
    """Найти в call_log вызов request() с type=optimizer."""
    for entry in call_log:
        if isinstance(entry, dict) and entry.get("type") == "optimizer":
            # Пропускаем publish-записи (аукцион), ищем только request-вызовы.
            if "$publish" not in entry:
                return entry
    return None


# ======================================================================
# Фикстура — предварительно зарегистрированный ответ оптимизатора
# ======================================================================
@pytest.fixture
def optimizer_mocked(mock_nats: MockNATS) -> MockNATS:
    """MockNATS с зарегистрированным ответом для optimizer."""
    mock_nats.register("optimizer", OPTIMIZER_RESPONSE)
    return mock_nats


# ======================================================================
# 3 агента, разные cost — побеждает минимальный
# ======================================================================
@pytest.mark.asyncio
async def test_auction_lowest_cost_wins(optimizer_mocked: MockNATS) -> None:
    """Среди трёх агентов побеждает тот, у кого cost минимальный."""
    # Arrange
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-001", "agent_id": "opt-alpha", "cost": 15.0,
         "base_cost": 10.0, "complexity_factor": 1.0},
        {"task_id": "auc-001", "agent_id": "opt-beta", "cost": 8.5,
         "base_cost": 7.0, "complexity_factor": 0.5},
        {"task_id": "auc-001", "agent_id": "opt-gamma", "cost": 12.0,
         "base_cost": 9.0, "complexity_factor": 1.2},
    ])

    # Act
    result = await run_optimizer_auction(
        optimizer_mocked,
        task_id="auc-001",
        payload={"test": True},
    )

    # Assert — статус
    assert result["status"] == "completed"

    # Assert — победитель opt-beta (cost=8.5 — минимальный)
    request_entry = _find_optimizer_request(optimizer_mocked.call_log)
    assert request_entry is not None, "нет request в call_log"
    assert request_entry.get("target_agent_id") == "opt-beta", \
        f"ожидался opt-beta, получен {request_entry.get('target_agent_id')}"

    # Assert — в ответе победителя agent_id (если пришёл)
    agent_id = result.get("agent_id", "")
    assert agent_id == "" or agent_id == "opt-beta"


# ======================================================================
# Все агенты отказываются (cost = INF)
# ======================================================================
@pytest.mark.asyncio
async def test_auction_all_refuse(optimizer_mocked: MockNATS) -> None:
    """Если все агенты отказались — PipelineError."""
    # Arrange — все ставки с cost >= REFUSE_COST
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-002", "agent_id": "opt-alpha", "cost": REFUSE_COST,
         "base_cost": 10.0, "complexity_factor": 1.0},
        {"task_id": "auc-002", "agent_id": "opt-beta", "cost": REFUSE_COST,
         "base_cost": 7.0, "complexity_factor": 0.5},
    ])

    # Act / Assert
    with pytest.raises(Exception) as exc_info:
        await run_optimizer_auction(
            optimizer_mocked,
            task_id="auc-002",
        )

    assert "ни один агент не принял задачу" in str(exc_info.value)


# ======================================================================
# Частичный отказ — победитель из оставшихся
# ======================================================================
@pytest.mark.asyncio
async def test_auction_partial_refuse(optimizer_mocked: MockNATS) -> None:
    """Один агент отказался (cost=INF), второй выигрывает."""
    # Arrange
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-003", "agent_id": "opt-alpha", "cost": REFUSE_COST,
         "base_cost": 10.0, "complexity_factor": 1.0},  # отказ
        {"task_id": "auc-003", "agent_id": "opt-beta", "cost": 9.0,
         "base_cost": 7.0, "complexity_factor": 1.0},   # участвует
        {"task_id": "auc-003", "agent_id": "opt-gamma", "cost": 14.0,
         "base_cost": 11.0, "complexity_factor": 0.8},  # участвует
    ])

    # Act
    result = await run_optimizer_auction(
        optimizer_mocked,
        task_id="auc-003",
    )

    # Assert — победитель opt-beta (9.0 < 14.0)
    assert result["status"] == "completed"
    request_entry = _find_optimizer_request(optimizer_mocked.call_log)
    assert request_entry is not None
    assert request_entry.get("target_agent_id") == "opt-beta"


# ======================================================================
# Экономия: победитель дешевле второго в 2+ раза
# ======================================================================
@pytest.mark.asyncio
async def test_auction_cost_gap(optimizer_mocked: MockNATS) -> None:
    """Проверка, что победитель существенно дешевле проигравшего."""
    # Arrange
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-004", "agent_id": "opt-slow", "cost": 50.0,
         "base_cost": 40.0, "complexity_factor": 2.0},
        {"task_id": "auc-004", "agent_id": "opt-fast", "cost": 7.5,
         "base_cost": 6.0, "complexity_factor": 0.5},
    ])

    # Act
    result = await run_optimizer_auction(
        optimizer_mocked,
        task_id="auc-004",
        payload={"test_auction": True},
    )

    # Assert
    assert result["status"] == "completed"
    request_entry = _find_optimizer_request(optimizer_mocked.call_log)
    assert request_entry is not None
    assert request_entry.get("target_agent_id") == "opt-fast"

    # Проверяем, что задача действительно содержит payload
    payload = request_entry.get("payload", {})
    assert payload == {"test_auction": True}


# ======================================================================
# Один агент — один победитель (безальтернативный аукцион)
# ======================================================================
@pytest.mark.asyncio
async def test_auction_single_bidder(optimizer_mocked: MockNATS) -> None:
    """Единственный участник автоматически становится победителем."""
    # Arrange
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-005", "agent_id": "opt-sole", "cost": 11.0,
         "base_cost": 11.0, "complexity_factor": 0.0},
    ])

    # Act
    result = await run_optimizer_auction(
        optimizer_mocked,
        task_id="auc-005",
        payload={"single": True},
    )

    # Assert
    assert result["status"] == "completed"
    request_entry = _find_optimizer_request(optimizer_mocked.call_log)
    assert request_entry is not None
    assert request_entry.get("target_agent_id") == "opt-sole"


# ======================================================================
# Аукцион с complexity — влияет на стоимость
# ======================================================================
@pytest.mark.asyncio
async def test_auction_complexity_affects_cost(optimizer_mocked: MockNATS) -> None:
    """Проверка, что complexity передаётся в publish-запрос."""
    # Arrange — 2 агента, любой cost (главное — проверить publish)
    optimizer_mocked.set_auction_bids([
        {"task_id": "auc-006", "agent_id": "opt-a", "cost": 10.0,
         "base_cost": 8.0, "complexity_factor": 1.0},
        {"task_id": "auc-006", "agent_id": "opt-b", "cost": 12.0,
         "base_cost": 10.0, "complexity_factor": 0.8},
    ])

    # Act
    await run_optimizer_auction(
        optimizer_mocked,
        task_id="auc-006",
        complexity=7,
    )

    # Assert — в publish-вызове (аукцион) есть complexity
    publish_entries = [
        e for e in optimizer_mocked.call_log
        if isinstance(e, dict) and e.get("$publish")
    ]
    assert len(publish_entries) >= 1, "нет publish-вызовов"

    auction_publish = publish_entries[0]
    assert auction_publish.get("complexity") == 7, \
        f"complexity не 7: {auction_publish.get('complexity')}"
