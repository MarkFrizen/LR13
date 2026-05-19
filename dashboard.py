#!/usr/bin/env python3
"""
dashboard.py — Streamlit-дашборд для маркетинговой мультиагентной платформы.

Автообновление через st.empty() + time.sleep + st.rerun().

Запуск:
  streamlit run dashboard.py --server.port 8501
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import redis.asyncio as aioredis
import streamlit as st
from nats import connect

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_HTTP = os.getenv("NATS_HTTP", "http://localhost:8222")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8000")
JAEGER_UI_URL = os.getenv("JAEGER_UI_URL", "http://localhost:16686")
STREAM_NAME = os.getenv("SCALER_STREAM", "TASKS")
REFRESH_SEC = 5

# Тёмная тема задаётся в .streamlit/config.toml
st.set_page_config(
    page_title="Marketing Agents Dashboard (Dark)",
    page_icon="📊",
    layout="wide",
)

# ======================================================================
# Data-fetching functions (кэшируются на REFRESH_SEC сек)
# ======================================================================


@st.cache_data(ttl=REFRESH_SEC)
def _cached_conns() -> list[dict[str, Any]]:
    return _fetch_sync(_get_nats_connections())


@st.cache_data(ttl=REFRESH_SEC)
def _cached_stream() -> dict[str, Any] | None:
    return _fetch_sync(_get_stream_info())


@st.cache_data(ttl=REFRESH_SEC)
def _cached_pipelines() -> list[dict[str, Any]]:
    return _fetch_sync(_get_pipeline_history())


def _fetch_sync(coro):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    except Exception:
        return None


async def _get_nats_connections() -> list[dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{NATS_HTTP}/connz", params={"subs": "true"})
            resp.raise_for_status()
            return resp.json().get("connections", [])
    except Exception:
        return []


async def _get_stream_info() -> dict[str, Any] | None:
    try:
        nc = await connect(NATS_URL)
        js = nc.jetstream()
        info = await js.stream_info(STREAM_NAME)
        await nc.drain()
        return {
            "messages": info.state.messages,
            "bytes": info.state.bytes,
            "first_seq": info.state.first_seq,
            "last_seq": info.state.last_seq,
        }
    except Exception:
        return None


async def _get_pipeline_history(limit: int = 10) -> list[dict[str, Any]]:
    try:
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        cursor, keys = 0, []
        pipelines = []
        while True:
            cursor, keys = await r.scan(cursor, match="pipeline:*", count=100)
            for key in keys:
                raw = await r.get(key)
                if raw:
                    try:
                        pipelines.append(json.loads(raw))
                    except json.JSONDecodeError:
                        pass
            if cursor == 0:
                break
        await r.aclose()
        pipelines.sort(key=lambda p: p.get("pipeline_id", ""), reverse=True)
        return pipelines[:limit]
    except Exception:
        return []


def extract_agents(conns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    agents, seen = [], set()
    for conn in conns:
        name = conn.get("name", "") or ""
        subs_raw = conn.get("subscriptions_list", []) or conn.get("subscriptions", [])
        subs = [s.get("subject", s) if isinstance(s, dict) else str(s) for s in subs_raw]
        if not any("tasks.process" in s for s in subs):
            continue
        aid = conn.get("client_id", conn.get("cid", name))
        if aid in seen:
            continue
        seen.add(aid)
        agent_type = "optimizer" if any("auction" in s for s in subs) else "segment"
        agents.append({
            "id": aid, "name": name or f"agent-{aid}", "type": agent_type,
            "ip": conn.get("ip", ""), "port": conn.get("port", ""),
            "uptime": conn.get("uptime", ""), "subscriptions": len(subs),
            "subscriptions_list": subs,
        })
    return agents


# ======================================================================
# Sidebar (вне auto-refresh-контейнера — не пересоздаётся)
# ======================================================================
st.sidebar.header("ℹ️ О дашборде")
st.sidebar.markdown(
    f"""
**Обновление:** каждые {REFRESH_SEC} с

**Источники:**
- NATS API: `{NATS_HTTP}`
- NATS JetStream: `{NATS_URL}`
- Redis: `{REDIS_URL}`
- Оркестратор: `{ORCHESTRATOR_URL}`
- Jaeger: `{JAEGER_UI_URL}`
"""
)

# ======================================================================
# Основной контент — auto-refresh через st.empty() + time.sleep + st.rerun()
# ======================================================================
main_placeholder = st.empty()

with main_placeholder.container():
    st.title("📊 Marketing Agents Dashboard")
    st.caption(f"Автообновление каждые {REFRESH_SEC} с  ·  "
               f"NATS: {NATS_URL}  ·  Redis: {REDIS_URL}  ·  "
               f"Orchestrator: {ORCHESTRATOR_URL}")

    tab_dash, tab_test, tab_jaeger = st.tabs(["📊 Дашборд", "🧪 Тест", "🔍 Jaeger"])

    # ======================================================================
    # TAB 1: Дашборд
    # ======================================================================
    with tab_dash:
        conns = _cached_conns() or []
        stream = _cached_stream()
        pipelines = _cached_pipelines() or []
        agents = extract_agents(conns)

        # --- Метрики ---
        col1, col2, col3, col4 = st.columns(4)
        agent_types = {}
        for a in agents:
            agent_types[a["type"]] = agent_types.get(a["type"], 0) + 1
        type_help = ", ".join(f"{k}={v}" for k, v in agent_types.items())

        col1.metric("Активные агенты", len(agents), help=type_help or "—")
        col2.metric("NATS соединений", len(conns))
        col3.metric("Задач в стриме", stream["messages"] if stream else "N/A")
        col4.metric("Объём стрима", f"{stream['bytes']/1024/1024:.1f} MB" if stream else "N/A")

        # --- Активные агенты ---
        st.subheader("🤖 Активные агенты")
        if agents:
            st.dataframe(
                [{"ID": a["id"], "Имя": a["name"], "Тип": a["type"],
                  "IP": f"{a['ip']}:{a['port']}" if a.get('port') else a['ip'],
                  "Подписок": a["subscriptions"]}
                 for a in agents],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Нет активных агентов")

        # --- Подписки ---
        st.subheader("📡 Подписки на tasks.process")
        subs = [a for a in agents if any("tasks.process" in s for s in a["subscriptions_list"])]
        if subs:
            st.dataframe(
                [{"Агент": a["name"], "Тип": a["type"],
                  "IP": f"{a['ip']}:{a['port']}" if a.get('port') else a['ip'],
                  "Подписок": a["subscriptions"]}
                 for a in subs],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("Нет подписчиков")

        # --- Pipeline ---
        st.subheader("📋 Последние pipeline")
        if pipelines:
            rows = []
            for p in pipelines:
                stages = p.get("stages", [])
                last = stages[-1] if stages else {}
                r = last.get("result") or {}
                metrics = ""
                if "roi" in r:
                    metrics = f"ROI={r['roi']}% CTR={r.get('ctr', '?')}%"
                rows.append({
                    "ID": p.get("pipeline_id", "?"),
                    "Статус": p.get("status", "?"),
                    "Этапов": len(stages),
                    "Метрики": metrics,
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)

            with st.expander("🔍 Детали последнего", expanded=False):
                st.json(pipelines[0])
        else:
            st.info("Нет завершённых pipeline")

    # ======================================================================
    # TAB 2: Тест
    # ======================================================================
    with tab_test:
        st.subheader("🧪 Запустить тестовый pipeline")
        st.markdown(
            "Генерирует **50–200** случайных клиентов с разными возрастами и "
            "регионами, затем выполняет полный pipeline: "
            "**сегментация → рассылка → аналитика**."
        )

        if st.button("🚀 Запустить тестовую задачу", type="primary", use_container_width=True):
            with st.spinner("Выполнение pipeline (сегментация → рассылка → аналитика)..."):
                try:
                    resp = httpx.post(
                        f"{ORCHESTRATOR_URL}/test",
                        timeout=120,
                    )
                    resp.raise_for_status()
                    result = resp.json()
                    st.success(f"✅ Pipeline завершён! Статус: **{result.get('status', '?')}**")
                    st.json(result)
                except httpx.RequestError as exc:
                    st.error(f"❌ Ошибка подключения к оркестратору: {exc}")
                except Exception as exc:
                    st.error(f"❌ Ошибка: {exc}")

        st.divider()
        st.subheader("📋 Последние запуски")
        pipelines = _cached_pipelines() or []
        if pipelines:
            for p in pipelines[:5]:
                pid = p.get("pipeline_id", "?")
                status = p.get("status", "?")
                stages = p.get("stages", [])
                stage_info = ", ".join(
                    f"{s.get('name', '?')}={s.get('status', '?')}"
                    for s in stages
                )
                st.markdown(f"- **{pid}** — `{status}` — {stage_info}")

    # ======================================================================
    # TAB 3: Jaeger
    # ======================================================================
    with tab_jaeger:
        st.subheader("🔍 Jaeger Tracing")
        st.caption(
            f"Если Jaeger недоступен, убедитесь что он запущен "
            f"(`docker compose up -d jaeger`). "
            f"UI: {JAEGER_UI_URL}"
        )
        st.markdown(f"🔗 [Открыть Jaeger UI]({JAEGER_UI_URL})")
        st.components.v1.html(
            f"""
            <iframe src="{JAEGER_UI_URL}/search"
                    width="100%"
                    height="800"
                    style="border: 1px solid #555; border-radius: 8px;">
            </iframe>
            """,
            height=820,
            scrolling=True,
        )

# ======================================================================
# Автообновление — задержка + перезапуск скрипта
# ======================================================================
time.sleep(REFRESH_SEC)
st.rerun()
