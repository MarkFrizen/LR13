"""FastAPI-оркестратор маркетингового pipeline.

Запуск:
    NATS_URL=nats://localhost:4222 REDIS_URL=redis://localhost:6379/0 \\
        OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318/v1/traces \\
        uvicorn main:app --host 0.0.0.0 --port 8000

Pipeline:
    1. POST /pipeline          — запустить pipeline с переданными клиентами
    2. POST /test              — запустить тестовый pipeline (случайные клиенты)
    3. GET  /pipeline/{id}     — получить статус pipeline
    4. GET  /health            — проверка состояния
"""

from __future__ import annotations

import logging
import os
import random
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, HTTPException
from nats.aio.client import Client as NATS
from redis.asyncio import Redis

from models import Client, PipelineRequest, PipelineResponse
from otel_setup import init_otel
from pipeline import run_pipeline
from pipeline_store import PipelineStore

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Глобальные синглтоны
# ---------------------------------------------------------------------------
nats_client: NATS | None = None
redis_client: Redis | None = None
store: PipelineStore | None = None
tracer_provider: object | None = None


async def get_nats() -> NATS:
    """Вернуть подключение к NATS."""
    assert nats_client is not None, "NATS не инициализирован"
    return nats_client


async def get_store() -> PipelineStore:
    """Вернуть хранилище pipeline."""
    assert store is not None, "PipelineStore не инициализирован"
    return store


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Подключение к NATS, Redis и инициализация OTel при старте."""
    global nats_client, redis_client, store, tracer_provider

    # --- OpenTelemetry (должен быть первым — до инициализации HTTP-роутов) ---
    tracer_provider = init_otel(app, service_name="orchestrator")
    logger.info("OpenTelemetry: FastAPI instrumented, service=orchestrator")

    # --- NATS ---
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    logger.info("подключение к NATS: %s", nats_url)
    nats_client = NATS()
    try:
        await nats_client.connect(
            nats_url,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
        )
        logger.info("подключение к NATS установлено")
    except Exception as exc:
        logger.error("не удалось подключиться к NATS: %s", exc)
        raise

    # --- Redis ---
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    logger.info("подключение к Redis: %s", redis_url)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await redis_client.ping()
        logger.info("подключение к Redis установлено")
    except Exception as exc:
        logger.error("не удалось подключиться к Redis: %s", exc)
        logger.warning("Redis недоступен, сохранение статуса отключено")

    store = PipelineStore(redis_client)

    yield

    logger.info("отключение внешних сервисов...")
    await nats_client.drain()
    await nats_client.close()
    nats_client = None
    await redis_client.aclose()
    redis_client = None
    # Shutdown TracerProvider — сброс буфера spans.
    if tracer_provider is not None:
        tracer_provider.shutdown()
        tracer_provider = None
    logger.info("отключение завершено")


# ---------------------------------------------------------------------------
# FastAPI-приложение
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Marketing Orchestrator",
    description="Оркестратор маркетинговых pipeline — "
                "сегментация → рассылка → аналитика (+оптимизация при низком ROI)",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Эндпоинты
# ---------------------------------------------------------------------------
@app.get("/health")
async def health() -> dict[str, str]:
    """Проверка состояния оркестратора и зависимостей."""
    nc = await get_nats()
    if not nc.is_connected:
        raise HTTPException(status_code=503, detail="NATS не подключён")

    s = await get_store()
    redis_ok = False
    try:
        await s.redis.ping()
        redis_ok = True
    except Exception:
        pass

    return {
        "status": "ok",
        "nats": "connected",
        "redis": "connected" if redis_ok else "unavailable",
    }


@app.get("/pipeline/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: str) -> PipelineResponse:
    """Получить сохранённый статус pipeline по ID."""
    s = await get_store()
    result = await s.get(pipeline_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline {pipeline_id} не найден",
        )
    return result


@app.post("/pipeline", response_model=PipelineResponse)
async def start_pipeline(request: PipelineRequest) -> PipelineResponse:
    """Запустить полный pipeline.

    Этапы:
      1. **Сегментация** — деление клиентов на группы (возраст/регион).
      2. **Рассылка** — отправка кампаний по каждому сегменту.
      3. **Аналитика** — сбор метрик отклика (CTR, ROI).
      4. *(условно)* **Оптимизация** — если ROI < 10%, корректировка бюджета.
      5. *(условно)* **Повторная аналитика** — после оптимизации.

    Результат каждого этапа сохраняется в Redis.
    """
    logger.info(
        "POST /pipeline: clients=%d channel=%s",
        len(request.clients),
        request.campaign_channel,
    )

    nc = await get_nats()
    if not nc.is_connected:
        raise HTTPException(status_code=503, detail="NATS не подключён")

    s = await get_store()
    clients_dicts = [c.model_dump() for c in request.clients]

    try:
        result = await run_pipeline(
            nc=nc,
            clients=clients_dicts,
            campaign_channel=request.campaign_channel,
            save_state=s.save,
        )
    except Exception as exc:
        logger.exception("непредвиденная ошибка pipeline")
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@app.post("/test", response_model=PipelineResponse)
async def test_pipeline() -> PipelineResponse:
    """Запустить тестовый pipeline со случайными клиентами.

    Генерирует 50–200 случайных клиентов с разными возрастами и регионами,
    затем выполняет полный pipeline (сегментация → рассылка → аналитика).
    """
    regions = ["MSK", "SPB", "EKB", "NSK", "KZN", "RND", "SAM", "UFA"]
    names = [
        "Иван", "Мария", "Алексей", "Ольга", "Дмитрий", "Елена",
        "Сергей", "Анна", "Андрей", "Татьяна", "Николай", "Юлия",
    ]
    count = random.randint(50, 200)

    clients = [
        {
            "id": i,
            "name": random.choice(names),
            "age": random.randint(16, 65),
            "region": random.choice(regions),
            "purchases": random.randint(0, 50),
        }
        for i in range(1, count + 1)
    ]

    logger.info("POST /test: сгенерировано %d клиентов", len(clients))

    nc = await get_nats()
    if not nc.is_connected:
        raise HTTPException(status_code=503, detail="NATS не подключён")

    s = await get_store()

    try:
        result = await run_pipeline(
            nc=nc,
            clients=clients,
            campaign_channel="email",
            save_state=s.save,
        )
    except Exception as exc:
        logger.exception("ошибка тестового pipeline")
        raise HTTPException(status_code=500, detail=str(exc))

    return result


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("RELOAD", "false").lower() == "true",
    )
