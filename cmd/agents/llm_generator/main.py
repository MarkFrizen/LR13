#!/usr/bin/env python3
"""
llm_generator — агент генерации текста рассылок через Ollama.

Подписывается на ``tasks.process``, обрабатывает задачи типа
``generate_copy``, генерирует текст через локальную LLM (llama3)
и публикует результат в ``tasks.completed``.

Возможности:
  - кэширование результатов в Redis (по хэшу topic+tone);
  - fallback на шаблонный текст при недоступности Ollama.

Запуск:
  OLLAMA_URL=http://ollama:11434 python main.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import uuid
from typing import Any

import httpx
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

# ---------------------------------------------------------------------------
# Конфигурация (переопределяется через env)
# ---------------------------------------------------------------------------
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "15"))  # сек

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # 1 час

FALLBACK_TEMPLATE = os.getenv(
    "FALLBACK_TEMPLATE",
    "Уникальное предложение для вас! Специальная акция — успейте воспользоваться.",
)

SUBSCRIBE_SUBJECT = "tasks.process"
PUBLISH_SUBJECT = "tasks.completed"
ERROR_SUBJECT = "tasks.error"
TASK_TYPE = "generate_copy"

_INSTANCE_ID = uuid.uuid4().hex[:8]
logger = logging.getLogger("llm_generator")


# ======================================================================
# Redis — кэш сгенерированных текстов
# ======================================================================

def _init_redis() -> Any | None:
    """Подключиться к Redis. Возвращает None при недоступности."""
    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(REDIS_URL, decode_responses=True)
        return client
    except Exception as exc:
        logger.warning("Redis недоступен, кэш отключён: %s", exc)
        return None


def _cache_key(topic: str, tone: str) -> str:
    """Сгенерировать ключ кэша по теме и тональности."""
    raw = f"{topic}||{tone}"
    h = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"llm:copy:{h}"


async def _get_cached(redis: Any | None, key: str) -> str | None:
    """Получить кэшированный текст. None = miss."""
    if redis is None:
        return None
    try:
        text = await redis.get(key)
        if text:
            logger.info("кэш: HIT key=%s (%d символов)", key, len(text))
            return text
        logger.debug("кэш: MISS key=%s", key)
        return None
    except Exception as exc:
        logger.warning("кэш: ошибка чтения: %s", exc)
        return None


async def _set_cache(redis: Any | None, key: str, text: str) -> None:
    """Сохранить текст в кэш."""
    if redis is None:
        return
    try:
        await redis.set(key, text, ex=CACHE_TTL)
        logger.debug("кэш: сохранён key=%s ttl=%ds (%d символов)", key, CACHE_TTL, len(text))
    except Exception as exc:
        logger.warning("кэш: ошибка записи: %s", exc)


# ======================================================================
# Промпт-инжиниринг
# ======================================================================

def build_prompt(topic: str, tone: str, **kwargs: Any) -> str:
    """Собрать промпт для LLM на основе параметров задачи."""
    tone_desc = {
        "formal": (
            "используй официально-деловой стиль, обращение на «Вы», "
            "без сленга и эмодзи"
        ),
        "informal": (
            "используй дружеский, неформальный тон, "
            "можно эмодзи и разговорные выражения"
        ),
        "promotional": (
            "используй яркий, рекламный стиль, "
            "восклицательные предложения, эмодзи, "
            "акцент на выгоде и срочности"
        ),
    }
    style = tone_desc.get(tone, tone_desc["informal"])

    return (
        f"Ты — копирайтер маркетингового агентства. "
        f"Напиши текст email-рассылки на тему: «{topic}».\n\n"
        f"Требования:\n"
        f"- {style}\n"
        f"- объём: 2–4 абзаца\n"
        f"- структура: заголовок, вступление, основная часть, призыв к действию\n"
        f"- только текст, без лишних пояснений\n\n"
        f"Текст:"
    )


# ======================================================================
# HTTP-клиент для Ollama
# ======================================================================

async def call_ollama(prompt: str, model: str = OLLAMA_MODEL) -> str:
    """Отправить запрос в Ollama API и получить сгенерированный текст.

    Args:
        prompt: Текст промпта.
        model: Имя модели в Ollama.

    Returns:
        Сгенерированный текст.

    Raises:
        RuntimeError: при ошибке HTTP или пустом ответе.
    """
    url = f"{OLLAMA_URL}/api/generate"
    payload = {"model": model, "prompt": prompt, "stream": False}

    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
        logger.info("ollama: запрос model=%s prompt_len=%d", model, len(prompt))
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
        except httpx.TimeoutException:
            raise RuntimeError(f"Ollama timeout ({OLLAMA_TIMEOUT}s)")
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(f"Ollama HTTP {exc.response.status_code}: {exc.response.text}")
        except httpx.RequestError as exc:
            raise RuntimeError(f"Ollama connection error: {exc}")

        data = response.json()
        text = (data.get("response") or "").strip()

        if not text:
            raise RuntimeError("Ollama вернул пустой ответ")

        logger.info("ollama: ответ получен (%d символов)", len(text))
        return text


# ======================================================================
# Обработка задачи
# ======================================================================

async def handle_task(nc: NATS, msg: Msg, redis: Any | None) -> None:
    """Обработать входящее сообщение из NATS."""
    log = logger

    # Десериализация.
    try:
        task = json.loads(msg.data)
    except json.JSONDecodeError as exc:
        log.error("ошибка парсинга JSON: %s", exc)
        await publish_error(nc, msg, "", "невалидный JSON")
        return

    task_id = task.get("task_id", "unknown")
    task_type = task.get("type", "")
    payload = task.get("payload", {})

    log = logging.LoggerAdapter(logger, {"task_id": task_id})
    log.info("получена задача type=%s", task_type)

    # Фильтр по типу.
    if task_type != TASK_TYPE:
        log.warning("пропуск — неверный тип: %s", task_type)
        return

    # Извлечение параметров.
    topic = payload.get("topic", "").strip()
    tone = payload.get("tone", "informal").strip()

    if not topic:
        log.error("пустая тема")
        await publish_error(nc, msg, task_id, "не указана тема (topic)")
        return

    log.info("генерация: topic='%s' tone='%s'", topic, tone)

    # ──────────────────────────────────────────────────────────────────
    # Попытка чтения из кэша.
    # ──────────────────────────────────────────────────────────────────
    cache_key = _cache_key(topic, tone)
    cached = await _get_cached(redis, cache_key)
    if cached:
        generated_text = cached
        source = "cache"
    else:
        source = "llm"
        # ──────────────────────────────────────────────────────────────
        # Генерация через Ollama с fallback на шаблон.
        # ──────────────────────────────────────────────────────────────
        try:
            prompt = build_prompt(topic, tone, **payload)
            generated_text = await call_ollama(prompt)
        except RuntimeError as exc:
            log.error("ollama недоступен, использован fallback: %s", exc)
            generated_text = FALLBACK_TEMPLATE
            source = "fallback"

        # Сохраняем в кэш (только если не fallback — шаблон и так известен).
        if source == "llm":
            await _set_cache(redis, cache_key, generated_text)

    # Публикация результата.
    result = {
        "task_id": task_id,
        "type": TASK_TYPE,
        "status": "completed",
        "generated_text": generated_text,
        "model": OLLAMA_MODEL if source != "fallback" else "fallback",
        "topic": topic,
        "tone": tone,
        "source": source,
    }
    await publish_result(nc, msg, result, log)

    log.info("генерация завершена (source=%s, %d символов)", source, len(generated_text))


async def publish_result(nc: NATS, msg: Msg, result: dict[str, Any], log: logging.Logger) -> None:
    """Опубликовать результат задачи."""
    data = json.dumps(result, ensure_ascii=False).encode()

    if msg.reply:
        await nc.publish(msg.reply, data)
        log.debug("результат отправлен через request-reply")
        return

    await nc.publish(PUBLISH_SUBJECT, data)
    log.debug("результат опубликован в %s", PUBLISH_SUBJECT)


async def publish_error(nc: NATS, msg: Msg, task_id: str, error_text: str) -> None:
    """Опубликовать ошибку задачи."""
    result = {
        "task_id": task_id,
        "type": TASK_TYPE,
        "status": "error",
        "error": error_text,
    }
    data = json.dumps(result, ensure_ascii=False).encode()

    if msg.reply:
        await nc.publish(msg.reply, data)
    await nc.publish(ERROR_SUBJECT, data)

    logger.error("задача %s: %s", task_id, error_text)


# ======================================================================
# Main
# ======================================================================

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] llm[%(instance)s] %(message)s",
    )
    global logger
    logger = logging.LoggerAdapter(logger, {"instance": _INSTANCE_ID})

    logger.info("запуск llm_generator (id=%s)", _INSTANCE_ID)
    logger.info("NATS: %s | Ollama: %s (%s) timeout=%ds",
                NATS_URL, OLLAMA_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT)

    # Redis (опциональный кэш).
    redis = _init_redis()
    if redis:
        logger.info("кэш Redis активен (ttl=%ds)", CACHE_TTL)
    else:
        logger.info("кэш Redis отключён")

    # NATS.
    nc = NATS()
    await nc.connect(NATS_URL, reconnect_time_wait=2, max_reconnect_attempts=-1)
    logger.info("подключение к NATS установлено")

    # Подписка.
    async def on_msg(msg: Msg) -> None:
        await handle_task(nc, msg, redis)

    sub = await nc.subscribe(SUBSCRIBE_SUBJECT, cb=lambda msg: asyncio.ensure_future(on_msg(msg)))
    logger.info("подписан на %s, ожидание задач типа '%s'", SUBSCRIBE_SUBJECT, TASK_TYPE)

    # Graceful shutdown.
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("получен сигнал завершения")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()

    logger.info("остановка агента...")
    await sub.unsubscribe()
    await nc.drain()
    await nc.close()
    if redis:
        await redis.aclose()
    logger.info("агент завершил работу")


if __name__ == "__main__":
    asyncio.run(main())
