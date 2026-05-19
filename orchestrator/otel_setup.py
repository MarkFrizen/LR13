"""Настройка OpenTelemetry для оркестратора.

Инициализирует TracerProvider с OTLP gRPC-экспортером
и подключает auto-instrumentation для FastAPI.

Схема:
  оркестратор ──OTLP gRPC──► otel-collector (:4317) ──► Jaeger
"""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger("orchestrator.otel")


def init_otel(app: FastAPI, service_name: str = "orchestrator") -> TracerProvider:
    """Инициализировать OpenTelemetry.

    1. Создаёт TracerProvider с OTLP gRPC-экспортером.
       Точка отправки задаётся переменной ``OTEL_EXPORTER_OTLP_ENDPOINT``
       (по умолчанию ``http://localhost:4317`` — OTLP gRPC-порт).
    2. Подключает auto-instrumentation для FastAPI —
       каждый HTTP-запрос получает root span.
    3. Устанавливает глобальный TracerProvider.

    Args:
        app: Экземпляр FastAPI для инструментирования.
        service_name: Имя сервиса (ресурс) в Jaeger.

    Returns:
        TracerProvider (нужен для корректного shutdown).
    """
    endpoint = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://localhost:4317",
    )

    resource = Resource.create({SERVICE_NAME: service_name})

    exporter = OTLPSpanExporter(endpoint=endpoint)

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter))

    trace.set_tracer_provider(provider)

    # FastAPI auto-instrumentation: root span на каждый HTTP-запрос
    # (метод, путь, статус-код, длительность).
    FastAPIInstrumentor.instrument_app(app)

    logger.info(
        "OpenTelemetry инициализирован: service=%s exporter=otlp-grpc endpoint=%s",
        service_name,
        endpoint,
    )

    return provider
