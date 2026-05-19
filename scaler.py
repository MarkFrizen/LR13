#!/usr/bin/env python3
"""
scaler.py — автоматическое масштабирование агента сегментации.

Метрики (каждые 15 с):
  1. NATS JetStream — длина очереди tasks.process
  2. Prometheus — CPU контейнеров агента (container_cpu_usage_seconds_total)

Условия масштабирования:
  - очередь > 100 сообщений                    → +1 реплика
  - средний CPU любого агента > 0.7 за 1 мин   → +1 реплика
  - очередь < 10 сообщений и CPU < 0.3         → -1 реплика

Бекенды:
  - Kubernetes (kubectl scale) если есть KUBECONFIG
  - Docker Compose (docker compose up --scale) по умолчанию

Блокировка:
  Redis-лок ``scaler:lock:segmentation`` (TTL 30 с) — предотвращает
  одновременное масштабирование несколькими экземплярами скалера.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import docker
import httpx
from nats import connect
from nats.js import JetStreamContext

# ======================================================================
# Конфигурация (переопределяется через env)
# ======================================================================

# --- NATS ---
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.getenv("SCALER_STREAM", "TASKS")
STREAM_SUBJECTS = ["tasks.process", "tasks.completed", "tasks.error"]

# --- Prometheus ---
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
PROMETHEUS_INTERVAL = 15  # секунд между запросами
CPU_THRESHOLD = 0.7
CPU_WINDOW = 4  # 4 семпла × 15 с ≈ 1 минута

# --- Redis (блокировка) ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LOCK_KEY = "scaler:lock:segmentation"
LOCK_TTL = 30

# --- Пороги масштабирования ---
QUEUE_UP = 100      # scale up если сообщений больше
QUEUE_DOWN = 10     # scale down если сообщений меньше
CPU_DOWN_THRESHOLD = 0.3
MIN_REPLICAS = 1
MAX_REPLICAS = 5
MAIN_INTERVAL = 15  # основной цикл (сек)

# --- Сервис ---
SERVICE_NAME = os.getenv("SCALER_SERVICE", "segmentation")

# --- Kubernetes ---
KUBECONFIG = os.getenv("KUBECONFIG", "")
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
K8S_DEPLOYMENT = os.getenv("K8S_DEPLOYMENT", "agent-segmentation")

# --- Идентификатор инстанса ---
_INSTANCE_ID = uuid.uuid4().hex[:8]
_PROJECT_DIR = Path(__file__).parent.resolve()

# ======================================================================
# Логирование
# ======================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] scaler[%(instance)s] %(message)s",
)
_logger = logging.getLogger("scaler")
logger = logging.LoggerAdapter(_logger, {"instance": _INSTANCE_ID})


# ======================================================================
# Блокировка (Redis)
# ======================================================================

def _init_redis() -> Any | None:
    try:
        import redis.asyncio as aioredis
        return aioredis.from_url(REDIS_URL, decode_responses=True)
    except Exception as exc:
        logger.warning("Redis недоступен, блокировка отключена: %s", exc)
        return None


async def acquire_lock(rc: Any | None) -> bool:
    if rc is None:
        return True
    try:
        ok = await rc.set(LOCK_KEY, _INSTANCE_ID, nx=True, ex=LOCK_TTL)
        return bool(ok)
    except Exception as exc:
        logger.warning("ошибка захвата лока: %s", exc)
        return True


async def release_lock(rc: Any | None) -> None:
    if rc is None:
        return
    try:
        script = """
        if redis.call("GET",KEYS[1])==ARGV[1] then
            return redis.call("DEL",KEYS[1])
        end
        return 0
        """
        await rc.eval(script, 1, LOCK_KEY, _INSTANCE_ID)
    except Exception as exc:
        logger.warning("ошибка освобождения лока: %s", exc)


# ======================================================================
# Prometheus — CPU-метрики контейнеров
# ======================================================================

async def query_prometheus(promql: str) -> list[dict[str, Any]]:
    """Выполнить PromQL-запрос, вернуть список результатов."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": promql},
            )
            resp.raise_for_status()
            data = resp.json()
            if data["status"] != "success":
                logger.warning("Prometheus вернул status=%s", data["status"])
                return []
            return data["data"]["result"]
    except Exception as exc:
        logger.debug("Prometheus недоступен: %s", exc)
        return []


async def get_container_cpu() -> dict[str, float]:
    """Вернуть ``{container_name: cpu_usage}``.

    CPU usage — доля ядра (0..N), усреднённая за 1 минуту.
    """
    # Пробуем несколько вариантов метрики (cAdvisor, Docker, kubelet).
    queries = [
        # Docker / cAdvisor
        f'avg by (name) (rate(container_cpu_usage_seconds_total{{name=~".*{SERVICE_NAME}.*"}}[1m]))',
        # Kubernetes / kubelet
        f'avg by (container) (rate(container_cpu_usage_seconds_total{{container=~".*{SERVICE_NAME}.*"}}[1m]))',
        # cAdvisor (альтернативный label)
        f'avg by (id) (rate(container_cpu_usage_seconds_total{{id=~".*/{SERVICE_NAME}.*"}}[1m]))',
    ]

    for q in queries:
        results = await query_prometheus(q)
        if results:
            out: dict[str, float] = {}
            for r in results:
                metric = r.get("metric", {})
                name = (
                    metric.get("name")
                    or metric.get("container")
                    or metric.get("id", "unknown")
                )
                try:
                    val = float(r["value"][1])
                    out[name] = val
                except (KeyError, ValueError, TypeError):
                    continue
            return out

    return {}


async def get_container_memory() -> dict[str, float]:
    """Вернуть ``{container_name: memory_bytes}``."""
    results = await query_prometheus(
        f'container_memory_usage_bytes{{name=~".*{SERVICE_NAME}.*"}}'
    )
    out: dict[str, float] = {}
    for r in results:
        name = r.get("metric", {}).get("name", "unknown")
        try:
            out[name] = float(r["value"][1])
        except (KeyError, ValueError, TypeError):
            continue
    return out


# ======================================================================
# NATS JetStream — длина очереди
# ======================================================================

async def ensure_stream(js: JetStreamContext) -> None:
    try:
        await js.stream_info(STREAM_NAME)
    except Exception:
        logger.info("создание стрима %s", STREAM_NAME)
        await js.add_stream(
            name=STREAM_NAME,
            subjects=STREAM_SUBJECTS,
            max_msgs=10_000,
            max_age=3600,
        )


async def get_queue_depth(js: JetStreamContext) -> int:
    try:
        info = await js.stream_info(STREAM_NAME)
        return info.state.messages
    except Exception:
        return 0


# ======================================================================
# Бекенды масштабирования
# ======================================================================

def _detect_backend() -> str:
    """Автоопределение: kubernetes или docker."""
    # 1. Явная настройка
    env = os.getenv("SCALER_BACKEND", "").lower()
    if env in ("k8s", "kubernetes"):
        return "k8s"
    if env == "docker":
        return "docker"

    # 2. KUBECONFIG задан и файл существует
    if KUBECONFIG and Path(KUBECONFIG).exists():
        return "k8s"
    if Path(Path.home() / ".kube" / "config").exists():
        return "k8s"

    # 3. Внутри Kubernetes (KUBERNETES_SERVICE_HOST)
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        return "k8s"

    return "docker"


async def get_current_replicas() -> int:
    """Вернуть текущее количество реплик сервиса."""
    backend = _detect_backend()
    if backend == "k8s":
        return await _k8s_get_replicas()
    return _docker_get_replicas()


async def scale_service(replicas: int) -> bool:
    """Масштабировать сервис до указанного количества реплик."""
    backend = _detect_backend()
    logger.info("масштабирование %s → %d реплик (backend=%s)", SERVICE_NAME, replicas, backend)
    if backend == "k8s":
        return await _k8s_scale(replicas)
    return _docker_scale(replicas)


# ---- Docker ----

def _docker_get_replicas() -> int:
    try:
        client = docker.from_env()
        containers = client.containers.list(
            filters={"label": f"com.docker.compose.service={SERVICE_NAME}"},
        )
        return len(containers)
    except Exception:
        return _docker_shell_count()


def _docker_shell_count() -> int:
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", f"label=com.docker.compose.service={SERVICE_NAME}",
             "--format", "{{.ID}}"],
            capture_output=True, text=True, cwd=_PROJECT_DIR,
        )
        lines = r.stdout.strip().splitlines()
        return len(lines) if lines[0] else 0
    except Exception:
        return 0


def _docker_scale(replicas: int) -> bool:
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d",
             "--scale", f"{SERVICE_NAME}={replicas}", "--no-recreate"],
            check=True, cwd=_PROJECT_DIR, capture_output=True, text=True,
        )
        return True
    except subprocess.CalledProcessError as exc:
        logger.error("ошибка docker compose: %s", exc.stderr)
        return False


# ---- Kubernetes ----

def _kubectl_args() -> list[str]:
    args = ["kubectl"]
    if KUBECONFIG:
        args.extend(["--kubeconfig", KUBECONFIG])
    return args


async def _k8s_get_replicas() -> int:
    try:
        r = await asyncio.create_subprocess_exec(
            *_kubectl_args(), "get", "deployment", K8S_DEPLOYMENT,
            "-n", K8S_NAMESPACE, "-o", "json",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await r.communicate()
        data = json.loads(stdout)
        return int(data["spec"]["replicas"])
    except Exception as exc:
        logger.warning("kubectl get deployment: %s", exc)
        return 0


async def _k8s_scale(replicas: int) -> bool:
    try:
        r = await asyncio.create_subprocess_exec(
            *_kubectl_args(), "scale", "deployment", K8S_DEPLOYMENT,
            "--replicas", str(replicas),
            "-n", K8S_NAMESPACE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await r.communicate()
        if r.returncode != 0:
            logger.error("kubectl scale: %s", stderr.decode().strip())
            return False
        return True
    except Exception as exc:
        logger.error("kubectl scale: %s", exc)
        return False


# ======================================================================
# Основной цикл
# ======================================================================

async def main_loop() -> None:
    logger.info("запуск scaler (id=%s)", _INSTANCE_ID)
    backend = _detect_backend()
    logger.info("бекенд: %s | NATS: %s | Redis: %s | Prometheus: %s",
                backend, NATS_URL, REDIS_URL, PROMETHEUS_URL)

    # ---- NATS ----
    nc = await connect(NATS_URL)
    js = nc.jetstream()
    await ensure_stream(js)
    logger.info("NATS подключён, стрим %s готов", STREAM_NAME)

    # ---- Redis ----
    redis_client = _init_redis()
    logger.info("Redis-блокировка: %s", "активна" if redis_client else "отключена")

    # ---- Prometheus (проверка) ----
    prom_ok = bool(await query_prometheus("up"))
    logger.info("Prometheus: %s", "доступен" if prom_ok else "недоступен")

    # Состояние CPU — скользящее окно на 1 минуту.
    cpu_window: deque[float] = deque(maxlen=CPU_WINDOW)
    last_scale_action: str | None = None

    logger.info(
        "цикл запущен (интервал=%dс | очередь >%d / CPU >%.1f | реплики %d..%d)",
        MAIN_INTERVAL, QUEUE_UP, CPU_THRESHOLD, MIN_REPLICAS, MAX_REPLICAS,
    )

    while True:
        try:
            queue_depth = await get_queue_depth(js)
            replicas = await get_current_replicas()

            # Prometheus — CPU.
            cpu_metrics = await get_container_cpu()
            if cpu_metrics:
                max_cpu = max(cpu_metrics.values())
                cpu_window.append(max_cpu)
                for name, val in cpu_metrics.items():
                    logger.debug("  CPU %s: %.3f", name, val)
            else:
                cpu_window.append(0.0)
                max_cpu = 0.0

            avg_cpu = sum(cpu_window) / len(cpu_window) if cpu_window else 0.0
            window_full = len(cpu_window) == CPU_WINDOW

            logger.info(
                "очередь=%d | реплик=%d/%d | CPU[avg=%.2f max=%.2f окно=%d/%d]",
                queue_depth, replicas, MAX_REPLICAS,
                avg_cpu, max_cpu, len(cpu_window), CPU_WINDOW,
            )

            # Решение о масштабировании.
            cpu_high = window_full and avg_cpu > CPU_THRESHOLD
            queue_high = queue_depth > QUEUE_UP
            queue_low = queue_depth < QUEUE_DOWN
            cpu_low = window_full and avg_cpu < CPU_DOWN_THRESHOLD

            should_up = (queue_high or cpu_high) and replicas < MAX_REPLICAS
            should_down = queue_low and cpu_low and replicas > MIN_REPLICAS

            if should_up or should_down:
                new_replicas = replicas
                reason = ""
                if should_up:
                    new_replicas = min(replicas + 1, MAX_REPLICAS)
                    if queue_high and cpu_high:
                        reason = "очередь+CPU"
                    elif queue_high:
                        reason = "очередь"
                    else:
                        reason = "CPU"
                    direction = "⬆ up"
                else:
                    new_replicas = max(replicas - 1, MIN_REPLICAS)
                    reason = "очередь+CPU"
                    direction = "⬇ down"

                if not await acquire_lock(redis_client):
                    logger.info("пропуск %s — Redis-лок занят", direction)
                    await asyncio.sleep(MAIN_INTERVAL)
                    continue

                try:
                    logger.info("▶  %s %d→%d (причина: %s)", direction, replicas, new_replicas, reason)
                    if await scale_service(new_replicas):
                        logger.info("✓ масштабирование %s выполнено", direction)
                        last_scale_action = direction
                    else:
                        logger.error("✗ масштабирование %s не удалось", direction)
                finally:
                    await release_lock(redis_client)
            else:
                logger.debug("бездействие")

        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("непредвиденная ошибка")

        await asyncio.sleep(MAIN_INTERVAL)

    await nc.drain()
    if redis_client is not None:
        await redis_client.aclose()


# ======================================================================
# Точка входа
# ======================================================================

def main() -> None:
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("остановка по сигналу")
        sys.exit(0)


if __name__ == "__main__":
    main()
