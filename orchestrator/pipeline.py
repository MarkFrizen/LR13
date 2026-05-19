"""Логика последовательного pipeline: сегментация → рассылка → аналитика.

Если ROI < 0.1 — дополнительный цикл: оптимизация → повторная аналитика.

Интеграция с OpenTelemetry: каждый вызов NATS и весь pipeline
оборачиваются в spans с атрибутами задачи.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from nats.aio.client import Client as NATS
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from auctioneer import run_optimizer_auction
from models import PipelineResponse, PipelineStage
from nats_utils import PipelineError, send_and_wait, REQUEST_TIMEOUT, SUBJECT_TASKS

logger = logging.getLogger("orchestrator.pipeline")
tracer = trace.get_tracer(__name__)

ROI_THRESHOLD = 0.1

# LLM-генерация текста рассылок (переключается через env USE_LLM).
USE_LLM = os.getenv("USE_LLM", "").lower() in ("1", "true", "yes")
DEFAULT_MESSAGE_TEMPLATE = os.getenv(
    "CAMPAIGN_MESSAGE_TEMPLATE",
    "Здравствуйте! У нас отличные новости — специальное предложение только для вас.",
)

SaveStateFn = Callable[[PipelineResponse], Coroutine[Any, Any, None]]


# ──────────────────────────────────────────────────────────────────────
#  Вспомогательные функции
# ──────────────────────────────────────────────────────────────────────


async def _save_if_needed(
    save_state: SaveStateFn | None,
    pipeline_id: str,
    status: str,
    stages: list[PipelineStage],
) -> None:
    if save_state is None:
        return
    partial = PipelineResponse(
        pipeline_id=pipeline_id,
        status=status,
        stages=list(stages),
    )
    await save_state(partial)


# ──────────────────────────────────────────────────────────────────────
#  LLM-генерация текста рассылки
# ──────────────────────────────────────────────────────────────────────


async def generate_copy_text(
    nc: NATS,
    task_id: str,
    topic: str,
    tone: str = "informal",
) -> str:
    """Сгенерировать текст рассылки.

    Если ``USE_LLM=true`` — отправляет задачу ``generate_copy`` агенту LLM
    и возвращает полученный текст. Иначе возвращает шаблонное сообщение.

    Args:
        nc: Подключение к NATS.
        task_id: ID задачи для трассировки.
        topic: Тема рассылки.
        tone: Тональность (formal / informal / promotional).

    Returns:
        Текст сообщения.
    """
    if not USE_LLM:
        logger.info("[%s] LLM отключён, используется шаблон", task_id)
        return DEFAULT_MESSAGE_TEMPLATE

    logger.info("[%s] генерация текста через LLM: topic='%s' tone='%s'", task_id, topic, tone)

    gen_task = {
        "task_id": f"{task_id}-copy",
        "type": "generate_copy",
        "payload": {"topic": topic, "tone": tone},
    }

    try:
        result = await send_and_wait(nc, gen_task)
        text = (result.get("generated_text") or "").strip()
        if text:
            logger.info("[%s] LLM-текст получен (%d символов, source=%s)",
                        task_id, len(text), result.get("source", "?"))
            return text
        logger.warning("[%s] LLM вернул пустой текст, используется шаблон", task_id)
    except PipelineError as exc:
        logger.warning("[%s] LLM недоступен, используется шаблон: %s", task_id, exc)

    return DEFAULT_MESSAGE_TEMPLATE


# ──────────────────────────────────────────────────────────────────────
#  Основной pipeline
# ──────────────────────────────────────────────────────────────────────


async def run_pipeline(
    nc: NATS,
    clients: list[dict[str, Any]],
    campaign_channel: str = "email",
    save_state: SaveStateFn | None = None,
) -> PipelineResponse:
    """Выполнить pipeline: сегментация → рассылка → аналитика.

    Если ROI < {ROI_THRESHOLD}, запускается цикл оптимизации:
    оптимизация → повторная аналитика (однократно).

    Args:
        nc: Подключение к NATS.
        clients: Список клиентов для сегментации.
        campaign_channel: Канал рассылки.
        save_state: Асинхронный колбэк для сохранения состояния в Redis.

    Returns:
        PipelineResponse с результатами всех этапов.
    """
    pipeline_id = f"pipe-{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(timezone.utc)
    stages: list[PipelineStage] = []
    final_status = "completed"

    with tracer.start_as_current_span(
        "run_pipeline",
        attributes={
            "pipeline.id": pipeline_id,
            "client.count": len(clients),
            "channel": campaign_channel,
        },
    ) as pipeline_span:

        # ==============================================================
        # Этап 1: Сегментация аудитории
        # ==============================================================
        logger.info("[%s] этап 1: сегментация аудитории", pipeline_id)

        segment_task = {
            "task_id": f"{pipeline_id}-seg",
            "type": "segment",
            "clients": clients,
        }

        try:
            seg_result = await send_and_wait(nc, segment_task)
            segments = seg_result.get("segments", [])

            if not segments:
                stages.append(PipelineStage(
                    stage=1, name="segmentation", status="completed",
                    result={"segments_count": 0, "segments": []},
                ))
                await _save_if_needed(save_state, pipeline_id, "completed", stages)
                logger.warning("[%s] сегментация не дала результатов", pipeline_id)
                pipeline_span.set_attribute("pipeline.segments", 0)
                pipeline_span.set_status(Status(StatusCode.OK))
                return PipelineResponse(pipeline_id=pipeline_id, status="completed", stages=stages)

            stages.append(PipelineStage(
                stage=1,
                name="segmentation",
                status="completed",
                result={
                    "segments_count": len(segments),
                    "segments": [
                        {
                            "name": s["name"],
                            "description": s.get("description", ""),
                            "count": s["count"],
                        }
                        for s in segments
                    ],
                },
            ))
            await _save_if_needed(save_state, pipeline_id, "running", stages)
            pipeline_span.set_attribute("pipeline.segments", len(segments))
            logger.info(
                "[%s] сегментация завершена: %d сегментов",
                pipeline_id, len(segments),
            )
        except PipelineError as exc:
            stages.append(PipelineStage(
                stage=1, name="segmentation", status="failed", error=str(exc),
            ))
            await _save_if_needed(save_state, pipeline_id, "failed", stages)
            pipeline_span.set_status(Status(StatusCode.ERROR, str(exc)))
            return PipelineResponse(pipeline_id=pipeline_id, status="failed", stages=stages)

        # ==============================================================
        # Этап 2: Рассылка кампаний по каждому сегменту
        # ==============================================================
        logger.info("[%s] этап 2: рассылка кампаний по %d сегментам", pipeline_id, len(segments))

        campaign_results: list[dict[str, Any]] = []
        campaign_errors: list[dict[str, Any]] = []

        for idx, segment in enumerate(segments, start=1):
            seg_name = segment["name"]
            logger.info("[%s]   рассылка #%d: '%s'", pipeline_id, idx, seg_name)

            # Генерация текста сообщения (LLM или шаблон).
            message_text = await generate_copy_text(
                nc,
                task_id=f"{pipeline_id}-cmp-{idx}",
                topic=seg_name,
                tone=campaign_channel,
            )

            campaign_task = {
                "task_id": f"{pipeline_id}-cmp-{idx}",
                "type": "campaign",
                "payload": {
                    "segment_name": seg_name,
                    "segment_description": segment.get("description", ""),
                    "channel": campaign_channel,
                    "client_count": segment.get("count", 0),
                    "message_body": message_text,
                },
            }

            try:
                cmp_result = await send_and_wait(nc, campaign_task)
                campaign_results.append(cmp_result)
            except PipelineError as exc:
                logger.error(
                    "[%s]   ошибка рассылки #%d ('%s'): %s",
                    pipeline_id, idx, seg_name, exc,
                )
                campaign_errors.append({"segment": seg_name, "error": str(exc)})

        stage2_result: dict[str, Any] = {
            "channel": campaign_channel,
            "campaigns": [
                {
                    "segment": (
                        campaign_results[i].get("payload", {}).get("segment_name")
                        or f"segment_{i}"
                    ),
                    "sent": campaign_results[i].get("sent", 0),
                    "failed": campaign_results[i].get("failed", 0),
                    "message": (
                        campaign_results[i].get("payload", {}).get("message_body", "")
                        or campaign_results[i].get("message_body", "")
                    ),
                }
                for i in range(len(campaign_results))
            ],
        }
        if campaign_errors:
            stage2_result["errors"] = campaign_errors

        stage2_status = "completed"
        if not campaign_results and campaign_errors:
            stage2_status = "failed"
            final_status = "failed"
        elif campaign_errors:
            stage2_status = "partial"

        pipeline_span.set_attribute("pipeline.campaigns_total", len(campaign_results))
        pipeline_span.set_attribute("pipeline.campaigns_errors", len(campaign_errors))

        stages.append(PipelineStage(
            stage=2, name="campaign", status=stage2_status, result=stage2_result,
        ))
        await _save_if_needed(save_state, pipeline_id, "running", stages)

        # ==============================================================
        # Этап 3: Анализ отклика
        # ==============================================================
        analytics_result: dict[str, Any] | None = None
        needs_optimization = False

        if campaign_results:
            logger.info("[%s] этап 3: анализ отклика", pipeline_id)

            analytics_payload = {
                "segment_count": len(segments),
                "campaign_count": len(campaign_results),
                "total_sent": sum(r.get("sent", 0) for r in campaign_results),
                "total_failed": sum(r.get("failed", 0) for r in campaign_results),
            }

            analytics_task = {
                "task_id": f"{pipeline_id}-anl",
                "type": "analytics",
                "payload": analytics_payload,
            }

            try:
                anl_result = await send_and_wait(nc, analytics_task)
                analytics_result = {
                    "ctr": anl_result.get("ctr"),
                    "roi": anl_result.get("roi"),
                    "opens": anl_result.get("opens"),
                    "clicks": anl_result.get("clicks"),
                    "conversions": anl_result.get("conversions"),
                }

                roi = anl_result.get("roi", 0.0) or 0.0
                stages.append(PipelineStage(
                    stage=3, name="analytics", status="completed", result=analytics_result,
                ))
                pipeline_span.set_attribute("pipeline.roi_initial", roi)
                logger.info(
                    "[%s] аналитика: CTR=%.2f%% ROI=%.2f%%",
                    pipeline_id, anl_result.get("ctr", 0), roi,
                )

                if roi < ROI_THRESHOLD:
                    needs_optimization = True
                    logger.info(
                        "[%s] ROI=%.2f%% < %.2f%% → запуск оптимизации",
                        pipeline_id, roi, ROI_THRESHOLD,
                    )

            except PipelineError as exc:
                stages.append(PipelineStage(
                    stage=3, name="analytics", status="failed", error=str(exc),
                ))
                final_status = "failed"

        else:
            logger.warning("[%s] этап 3 пропущен: нет данных кампаний", pipeline_id)
            stages.append(PipelineStage(
                stage=3, name="analytics", status="skipped",
                result={"reason": "нет результатов кампаний для анализа"},
            ))

        await _save_if_needed(save_state, pipeline_id, "running", stages)

        # ==============================================================
        # Этап 4: Оптимизация (только если ROI < порога)
        # ==============================================================
        if needs_optimization:
            logger.info("[%s] этап 4: оптимизация кампании", pipeline_id)

            optimizer_task = {
                "task_id": f"{pipeline_id}-opt",
                "type": "optimizer",
                "payload": {
                    "current_roi": analytics_result.get("roi") if analytics_result else 0,
                    "current_ctr": analytics_result.get("ctr") if analytics_result else 0,
                    "campaign_count": len(campaign_results),
                },
            }

            try:
                opt_result = await run_optimizer_auction(
                    nc,
                    task_id=f"{pipeline_id}-opt",
                    payload=optimizer_task["payload"],
                )
                stages.append(PipelineStage(
                    stage=4, name="optimization", status="completed",
                    result={
                        "old_budget": opt_result.get("old_budget"),
                        "new_budget": opt_result.get("new_budget"),
                        "change_percent": opt_result.get("change_percent"),
                        "adjustment": opt_result.get("adjustment"),
                    },
                ))
                logger.info(
                    "[%s] оптимизация: бюджет изменён на %.2f%% (%s)",
                    pipeline_id,
                    opt_result.get("change_percent", 0),
                    opt_result.get("adjustment", "unknown"),
                )
                pipeline_span.set_attribute("pipeline.optimization_applied", True)
                await _save_if_needed(save_state, pipeline_id, "running", stages)

                # ======================================================
                # Этап 5: Повторная аналитика после оптимизации
                # ======================================================
                logger.info("[%s] этап 5: повторная аналитика после оптимизации", pipeline_id)

                analytics_retry_task = {
                    "task_id": f"{pipeline_id}-anl2",
                    "type": "analytics",
                    "payload": {
                        **analytics_payload,
                        "optimization_applied": True,
                        "budget_change_pct": opt_result.get("change_percent"),
                    },
                }

                try:
                    anl2_result = await send_and_wait(nc, analytics_retry_task)
                    stages.append(PipelineStage(
                        stage=5, name="analytics_retry", status="completed",
                        result={
                            "ctr": anl2_result.get("ctr"),
                            "roi": anl2_result.get("roi"),
                            "opens": anl2_result.get("opens"),
                            "clicks": anl2_result.get("clicks"),
                            "conversions": anl2_result.get("conversions"),
                        },
                    ))
                    pipeline_span.set_attribute(
                        "pipeline.roi_after_optimization",
                        anl2_result.get("roi", 0),
                    )
                    logger.info(
                        "[%s] повторная аналитика: CTR=%.2f%% ROI=%.2f%%",
                        pipeline_id,
                        anl2_result.get("ctr", 0),
                        anl2_result.get("roi", 0),
                    )
                    await _save_if_needed(save_state, pipeline_id, "running", stages)
                except PipelineError as exc:
                    stages.append(PipelineStage(
                        stage=5, name="analytics_retry", status="failed", error=str(exc),
                    ))
                    final_status = "failed"
                    await _save_if_needed(save_state, pipeline_id, "failed", stages)

            except PipelineError as exc:
                stages.append(PipelineStage(
                    stage=4, name="optimization", status="failed", error=str(exc),
                ))
                final_status = "failed"
                await _save_if_needed(save_state, pipeline_id, "failed", stages)

        pipeline_span.set_attribute("pipeline.status", final_status)
        pipeline_span.set_attribute("pipeline.stages_count", len(stages))

        await _save_if_needed(save_state, pipeline_id, final_status, stages)

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info(
            "[%s] pipeline завершён: status=%s duration=%.2fs stages=%d",
            pipeline_id, final_status, elapsed, len(stages),
        )

        if final_status == "failed":
            pipeline_span.set_status(Status(StatusCode.ERROR, "pipeline failed"))
        else:
            pipeline_span.set_status(Status(StatusCode.OK))

        return PipelineResponse(
            pipeline_id=pipeline_id,
            status=final_status,
            stages=stages,
        )
