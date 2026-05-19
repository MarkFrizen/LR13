"""Сохранение и получение статуса pipeline в Redis."""

from __future__ import annotations

import json
import logging
from typing import Any

from redis.asyncio import Redis

from models import PipelineResponse

logger = logging.getLogger("orchestrator.store")

PIPELINE_TTL = 3600  # срок жизни записи (1 час)


class PipelineStore:
    """Redis-хранилище статусов pipeline."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def save(self, response: PipelineResponse) -> None:
        """Сохранить (или обновить) состояние pipeline."""
        key = f"pipeline:{response.pipeline_id}"
        data: dict[str, Any] = response.model_dump(mode="json")
        await self.redis.set(key, json.dumps(data), ex=PIPELINE_TTL)
        logger.debug("сохранено состояние pipeline=%s status=%s", response.pipeline_id, response.status)

    async def get(self, pipeline_id: str) -> PipelineResponse | None:
        """Получить состояние pipeline по ID."""
        key = f"pipeline:{pipeline_id}"
        raw = await self.redis.get(key)
        if raw is None:
            return None
        return PipelineResponse(**json.loads(raw))
