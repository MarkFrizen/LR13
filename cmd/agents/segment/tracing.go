package main

import (
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// initTracer создаёт глобальный TracerProvider с экспортом в Jaeger.
// Возвращает TracerProvider для корректного завершения (Flush/Shutdown).
func initTracer() (*sdktrace.TracerProvider, error) {
	endpoint := os.Getenv("JAEGER_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:14268/api/traces"
	}

	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(
		jaeger.WithEndpoint(endpoint),
	))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("segment-agent"),
		)),
	)

	// Устанавливаем глобальный провайдер трейсинга.
	otel.SetTracerProvider(tp)

	// Пропагатор поддерживает W3C Trace Context и Baggage.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// natsHeaderCarrier реализует otel propagation.TextMapCarrier
// для чтения/записи контекста трассировки в NATS-заголовки.
type natsHeaderCarrier map[string][]string

func (c natsHeaderCarrier) Get(key string) string {
	if values, ok := c[key]; ok && len(values) > 0 {
		return values[0]
	}
	return ""
}

func (c natsHeaderCarrier) Set(key, value string) {
	c[key] = []string{value}
}

func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
