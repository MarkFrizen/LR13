"""Интеграционные тесты для pipeline с мокированным NATS.

Проверяется:
  - последовательность вызовов агентов;
  - условный переход при ROI < 0.1;
  - обработка ошибок (таймаут, ошибка агента, пустые сегменты);
  - сохранение состояния.
"""

from __future__ import annotations

from unittest.mock import ANY, AsyncMock

import pytest

from pipeline import run_pipeline
from tests.conftest import (
    HIGH_ROI_ANALYTICS,
    LOW_ROI_ANALYTICS,
    OPTIMIZER_RESPONSE,
    SAMPLE_CLIENTS,
    SEGMENT_EMPTY_RESPONSE,
    SEGMENT_ERROR_RESPONSE,
    SEGMENT_RESPONSE,
    MockNATS,
    campaign_response,
)


# ======================================================================
# Happy path: ROI >= 0.1, без оптимизации
# ======================================================================
@pytest.mark.asyncio
async def test_happy_path_no_optimization(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Pipeline с 2 сегментами и высоким ROI — ровно 3 этапа, без оптимизации."""
    # Arrange
    mock_nats.register("segment", SEGMENT_RESPONSE)
    mock_nats.register("campaign", campaign_response)
    mock_nats.register("analytics", HIGH_ROI_ANALYTICS)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert — итоговый статус
    assert result.status == "completed"

    # Assert — последовательность этапов
    stage_names = [s.name for s in result.stages]
    assert stage_names == ["segmentation", "campaign", "analytics"]
    assert len(stage_names) == 3

    # Assert — каждый этап успешен
    for s in result.stages:
        assert s.status == "completed", f"stage {s.name} failed"

    # Assert — результат аналитики содержит ROI
    analytics_result = result.stages[2].result
    assert analytics_result["roi"] == 12.3
    assert analytics_result["ctr"] == 8.5

    # Assert — последовательность вызовов NATS
    call_types = [c["type"] for c in mock_nats.call_log]
    assert call_types == ["segment", "campaign", "campaign", "analytics"]
    # 1 сегмент + 2 кампании (по числу сегментов) + 1 аналитика

    # Assert — колбэк сохранения вызывался после каждого этапа
    assert save_state_mock.call_count >= 3


# ======================================================================
# Условный переход: ROI < 0.1 → оптимизация + повторная аналитика
# ======================================================================
@pytest.mark.asyncio
async def test_optimization_triggered(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Низкий ROI запускает оптимизатор и повторную аналитику."""
    # Arrange
    mock_nats.register("segment", SEGMENT_RESPONSE)
    mock_nats.register("campaign", campaign_response)
    mock_nats.register("analytics", LOW_ROI_ANALYTICS)
    mock_nats.register("optimizer", OPTIMIZER_RESPONSE)

    # Регистрируем ответ для повторной аналитики (уже с высоким ROI).
    RETRY_ANALYTICS = {**LOW_ROI_ANALYTICS, "roi": 8.2, "ctr": 4.1}

    # Для второго вызова analytics нужно вернуть другой ответ.
    # Используем счётчик вызовов.
    analytics_call_count = 0

    def analytics_factory(task: dict) -> dict:
        nonlocal analytics_call_count
        analytics_call_count += 1
        if analytics_call_count == 1:
            return LOW_ROI_ANALYTICS
        return RETRY_ANALYTICS

    mock_nats.register("analytics", analytics_factory)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert — итоговый статус
    assert result.status == "completed"

    # Assert — 5 этапов (добавились optimizer + analytics_retry)
    stage_names = [s.name for s in result.stages]
    assert stage_names == [
        "segmentation",
        "campaign",
        "analytics",
        "optimization",
        "analytics_retry",
    ]
    assert len(stage_names) == 5

    # Assert — первый analytics показал низкий ROI
    assert result.stages[2].result["roi"] == 0.05

    # Assert — оптимизация применилась
    opt_result = result.stages[3].result
    assert opt_result["adjustment"] == "increase"
    assert opt_result["change_percent"] == 15.0

    # Assert — повторная аналитика дала новый ROI
    assert result.stages[4].result["roi"] == 8.2

    # Assert — последовательность вызовов NATS.
    # Аукцион добавляет publish-вызов "optimizer" перед request-вызовом.
    call_types = [c["type"] for c in mock_nats.call_log]
    assert call_types == [
        "segment",
        "campaign",
        "campaign",
        "analytics",      # 1-й вызов — низкий ROI
        "optimizer",      # publish (аукцион)
        "optimizer",      # request (победитель аукциона)
        "analytics",      # 2-й вызов — повторная аналитика
    ]


# ======================================================================
# Ошибка сегментации
# ======================================================================
@pytest.mark.asyncio
async def test_segmentation_error(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Если агент сегментации вернул ошибку — pipeline завершается с status=failed."""
    # Arrange
    mock_nats.register("segment", SEGMENT_ERROR_RESPONSE)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert
    assert result.status == "failed"
    assert len(result.stages) == 1
    assert result.stages[0].name == "segmentation"
    assert result.stages[0].status == "failed"
    assert "временная недоступность БД" in (result.stages[0].error or "")

    # Ни один другой агент не вызывался
    assert len(mock_nats.call_log) == 1


# ======================================================================
# Пустые сегменты
# ======================================================================
@pytest.mark.asyncio
async def test_empty_segments(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Если сегментация вернула пустой список — pipeline завершается досрочно."""
    # Arrange
    mock_nats.register("segment", SEGMENT_EMPTY_RESPONSE)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert
    assert result.status == "completed"
    assert len(result.stages) == 1
    assert result.stages[0].name == "segmentation"
    assert result.stages[0].result["segments_count"] == 0

    # Только один вызов — дальнейшие этапы не запускались
    assert len(mock_nats.call_log) == 1


# ======================================================================
# Таймаут при сегментации
# ======================================================================
@pytest.mark.asyncio
async def test_segmentation_timeout(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Если агент не ответил — pipeline завершается с failed."""
    # Arrange — None в качестве ответа эмулирует таймаут.
    mock_nats.register("segment", None)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert
    assert result.status == "failed"
    assert result.stages[0].status == "failed"
    assert "таймаут" in (result.stages[0].error or "").lower()


# ======================================================================
# Частичная неудача рассылок
# ======================================================================
@pytest.mark.asyncio
async def test_campaign_partial_failure(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Если один из campaign-агентов вернул ошибку — статус partial, pipeline продолжается."""
    # Arrange
    mock_nats.register("segment", SEGMENT_RESPONSE)

    campaign_calls = 0

    def campaign_mixed(task: dict) -> dict:
        nonlocal campaign_calls
        campaign_calls += 1
        if campaign_calls == 1:
            # Первая кампания успешна
            return campaign_response(task)
        # Вторая — ошибка
        return {
            "task_id": task.get("task_id", "cmp-fail"),
            "type": "campaign",
            "status": "error",
            "error": "превышен лимит отправки",
        }

    mock_nats.register("campaign", campaign_mixed)
    mock_nats.register("analytics", HIGH_ROI_ANALYTICS)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert
    assert result.status == "completed"  # аналитика выполнилась
    assert result.stages[1].status == "partial"
    assert len(result.stages[1].result.get("errors", [])) == 1
    assert result.stages[1].result["errors"][0]["segment"] == "Регион: MSK"


# ======================================================================
# Сохранение состояния
# ======================================================================
@pytest.mark.asyncio
async def test_save_state_called_after_each_stage(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """save_state вызывается после каждого этапа, включая условный."""
    # Arrange
    mock_nats.register("segment", SEGMENT_RESPONSE)
    mock_nats.register("campaign", campaign_response)
    mock_nats.register("analytics", LOW_ROI_ANALYTICS)
    mock_nats.register("optimizer", OPTIMIZER_RESPONSE)

    analytics_call_count = 0

    def analytics_retry_factory(task: dict) -> dict:
        nonlocal analytics_call_count
        analytics_call_count += 1
        if analytics_call_count == 1:
            return LOW_ROI_ANALYTICS
        return {**LOW_ROI_ANALYTICS, "roi": 8.2, "ctr": 4.1}

    mock_nats.register("analytics", analytics_retry_factory)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert — после каждого из 5 этапов был вызов save_state
    assert save_state_mock.call_count >= 5

    # Проверим, что последний вызов содержит финальный статус
    last_call_args = save_state_mock.call_args_list[-1][0][0]
    assert last_call_args.pipeline_id == result.pipeline_id
    assert last_call_args.status == "completed"
    assert len(last_call_args.stages) == 5

    # Промежуточный вызов (этап 2) должен иметь status="running"
    mid_call_args = save_state_mock.call_args_list[1][0][0]
    assert mid_call_args.status == "running"
    assert len(mid_call_args.stages) == 2


# ======================================================================
# Проверка, что условный переход НЕ срабатывает при высоком ROI
# ======================================================================
@pytest.mark.asyncio
async def test_optimizer_not_called_when_roi_high(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """При ROI >= 0.1 оптимизатор НЕ вызывается — ровно 3 этапа."""
    # Arrange
    mock_nats.register("segment", SEGMENT_RESPONSE)
    mock_nats.register("campaign", campaign_response)
    mock_nats.register("analytics", HIGH_ROI_ANALYTICS)

    # Регистрируем оптимизатор, но pipeline не должен его вызывать.
    mock_nats.register("optimizer", OPTIMIZER_RESPONSE)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS,
        save_state=save_state_mock,
    )

    # Assert — оптимизатор не вызывался
    call_types = [c["type"] for c in mock_nats.call_log]
    assert "optimizer" not in call_types
    assert len(result.stages) == 3


# ======================================================================
# Сквозной тест: интеграция всех компонентов
# ======================================================================
@pytest.mark.asyncio
async def test_full_pipeline_with_all_agents(
    mock_nats: MockNATS,
    save_state_mock: AsyncMock,
) -> None:
    """Полный pipeline с большим количеством клиентов и тремя сегментами."""
    # Arrange — 3 сегмента
    three_segments = {
        **SEGMENT_RESPONSE,
        "segments": [
            {"name": "Возраст: 18-25", "description": "Молодёжь", "count": 100},
            {"name": "Возраст: 26-35", "description": "Молодые взрослые", "count": 80},
            {"name": "Регион: SPB", "description": "Клиенты из SPB", "count": 60},
        ],
    }

    mock_nats.register("segment", three_segments)
    mock_nats.register("campaign", campaign_response)
    mock_nats.register("analytics", LOW_ROI_ANALYTICS)
    mock_nats.register("optimizer", OPTIMIZER_RESPONSE)

    analytics_calls = 0

    def analytics_multi(task: dict) -> dict:
        nonlocal analytics_calls
        analytics_calls += 1
        if analytics_calls == 1:
            return LOW_ROI_ANALYTICS
        return {**LOW_ROI_ANALYTICS, "roi": 8.2}

    mock_nats.register("analytics", analytics_multi)

    # Act
    result = await run_pipeline(
        nc=mock_nats,
        clients=SAMPLE_CLIENTS * 5,  # 25 клиентов
        save_state=save_state_mock,
    )

    # Assert — 5 этапов
    assert len(result.stages) == 5
    assert result.status == "completed"

    # Assert — 3 кампании (по числу сегментов)
    campaign_calls = [c for c in mock_nats.call_log if c["type"] == "campaign"]
    assert len(campaign_calls) == 3

    # Assert — оптимизация + аналитика вызваны
    assert any(c["type"] == "optimizer" for c in mock_nats.call_log)
    analytics_calls_actual = [c for c in mock_nats.call_log if c["type"] == "analytics"]
    assert len(analytics_calls_actual) == 2  # первая + повторная

    # Assert — в каждом сегменте кампании есть название сегмента
    for i, cc in enumerate(campaign_calls):
        payload = cc.get("payload", {})
        seg_name = three_segments["segments"][i]["name"]
        assert payload.get("segment_name") == seg_name
