"""Pydantic-модели для оркестратора маркетингового pipeline."""

from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class Client(BaseModel):
    """Данные клиента для сегментации."""

    id: int
    name: str
    age: int
    region: str
    purchases: int = 0


class PipelineRequest(BaseModel):
    """Входные данные для запуска pipeline."""

    clients: list[Client] = Field(..., min_length=1, description="Список клиентов для обработки")
    campaign_channel: str = Field(default="email", description="Канал рассылки")


class PipelineStage(BaseModel):
    """Результат одного этапа pipeline."""

    stage: int
    name: str
    status: str
    result: Any = None
    error: str | None = None


class PipelineStatus(str):
    """Статус pipeline."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PipelineResponse(BaseModel):
    """Ответ с результатами pipeline."""

    pipeline_id: str
    status: str
    stages: list[PipelineStage]


class PipelineError(BaseModel):
    """Ответ при ошибке."""

    detail: str
