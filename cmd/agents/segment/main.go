package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultNATSURL    = "nats://localhost:4222"
	subscribeSubject  = "tasks.process"
	publishSubject    = "tasks.completed"
	errorSubject      = "tasks.error"
	taskTypeSegment   = "segment"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("запуск агента сегментации")

	// Инициализация OpenTelemetry (Jaeger exporter).
	tp, err := initTracer()
	if err != nil {
		slog.Error("не удалось инициализировать TracerProvider", "error", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			slog.Error("ошибка shutdown TracerProvider", "error", err)
		}
	}()
	slog.Info("TracerProvider инициализирован",
		"exporter", "jaeger",
		"endpoint", os.Getenv("JAEGER_ENDPOINT"),
	)

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = defaultNATSURL
	}

	nc, err := nats.Connect(natsURL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		slog.Error("не удалось подключиться к NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("подключение к NATS установлено", "url", natsURL)

	sub, err := nc.Subscribe(subscribeSubject, func(msg *nats.Msg) {
		handleTask(nc, msg)
	})
	if err != nil {
		slog.Error("ошибка подписки на очередь",
			"subject", subscribeSubject,
			"error", err,
		)
		os.Exit(1)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			slog.Error("ошибка отписки", "error", err)
		}
	}()

	slog.Info("агент ожидает задачи",
		"subscribe", subscribeSubject,
		"publish", publishSubject,
		"error_queue", errorSubject,
		"task_type", taskTypeSegment,
	)

	// Graceful shutdown по SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	slog.Info("получен сигнал завершения, остановка агента")
	nc.Drain()
	slog.Info("агент завершил работу")
}

// handleTask обрабатывает входящее сообщение из NATS.
func handleTask(nc *nats.Conn, msg *nats.Msg) {
	log := slog.With("subject", msg.Subject)

	// Извлекаем контекст трассировки из заголовков NATS-сообщения.
	propagator := otel.GetTextMapPropagator()
	carrier := natsHeaderCarrier(msg.Header)
	ctx := propagator.Extract(context.Background(), carrier)

	// Создаём span для обработки задачи.
	tracer := otel.Tracer("segment-agent")
	ctx, span := tracer.Start(ctx, "process_task",
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination", msg.Subject),
			attribute.String("messaging.protocol", "nats"),
		),
	)
	defer span.End()

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Error("ошибка десериализации задачи", "error", err)
		span.RecordError(err)
		span.SetAttributes(attribute.String("error", err.Error()))
		publishError(ctx, nc, msg.Reply, "", "", "невалидный JSON: "+err.Error())
		return
	}

	// Добавляем атрибуты задачи в span.
	span.SetAttributes(
		attribute.String("task_id", task.TaskID),
		attribute.String("task_type", task.Type),
		attribute.Int("client_count", len(task.Clients)),
	)

	log = log.With("task_id", task.TaskID, "type", task.Type, "clients", len(task.Clients))
	log.Info("получена задача на сегментацию")

	// Фильтр: обрабатываем только задачи типа "segment".
	if task.Type != taskTypeSegment {
		log.Warn("пропуск задачи — неверный тип")
		span.SetAttributes(attribute.String("skip_reason", "wrong type"))
		publishError(ctx, nc, msg.Reply, task.TaskID, task.Type,
			"ожидался тип 'segment', получен '"+task.Type+"'")
		return
	}

	if len(task.Clients) == 0 {
		log.Error("пустой список клиентов")
		span.RecordError(nil)
		span.SetAttributes(attribute.String("error", "empty client list"))
		publishError(ctx, nc, msg.Reply, task.TaskID, task.Type,
			"список клиентов пуст")
		return
	}

	// Выполнение сегментации.
	segments := segment(task.Clients)
	log.Info("сегментация завершена", "segments", len(segments))

	span.SetAttributes(attribute.Int("segments_count", len(segments)))

	result := Result{
		TaskID:   task.TaskID,
		Type:     task.Type,
		Segments: segments,
		Status:   "completed",
	}

	publishResult(ctx, nc, msg, result, log)
}

// publishResult публикует успешный результат сегментации с
// контекстом трассировки в заголовках NATS-сообщения.
func publishResult(ctx context.Context, nc *nats.Conn, msg *nats.Msg, result Result, log *slog.Logger) {
	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации результата", "error", err)
		return
	}

	// Формируем заголовки с контекстом трассировки.
	outHeaders := make(nats.Header)
	propagator := otel.GetTextMapPropagator()
	carrier := natsHeaderCarrier(outHeaders)
	propagator.Inject(ctx, carrier)

	if msg.Reply != "" {
		// Request-reply: отвечаем напрямую отправителю с заголовками.
		outMsg := nats.Msg{
			Subject: msg.Reply,
			Header:  outHeaders,
			Data:    data,
		}
		if err := nc.PublishMsg(&outMsg); err != nil {
			log.Error("ошибка publish в reply", "reply", msg.Reply, "error", err)
		}
		log.Info("результат отправлен через request-reply",
			"reply", msg.Reply, "segments", len(result.Segments))
		return
	}

	// Публикация в очередь tasks.completed с заголовками.
	outMsg := nats.Msg{
		Subject: publishSubject,
		Header:  outHeaders,
		Data:    data,
	}
	if err := nc.PublishMsg(&outMsg); err != nil {
		log.Error("ошибка публикации результата",
			"subject", publishSubject, "error", err)
		return
	}

	log.Info("результат опубликован", "subject", publishSubject,
		"segments", len(result.Segments))

	for _, s := range result.Segments {
		log.Info("  сегмент", "name", s.Name, "count", s.Count)
	}
}

// publishError публикует ошибку обработки задачи с
// контекстом трассировки в заголовках NATS-сообщения.
func publishError(ctx context.Context, nc *nats.Conn, reply, taskID, taskType, errMsg string) {
	log := slog.With("task_id", taskID)

	result := Result{
		TaskID: taskID,
		Type:   taskType,
		Status: "error",
		Error:  errMsg,
	}

	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации ошибки", "error", err)
		return
	}

	// Формируем заголовки с контекстом трассировки.
	outHeaders := make(nats.Header)
	propagator := otel.GetTextMapPropagator()
	carrier := natsHeaderCarrier(outHeaders)
	propagator.Inject(ctx, carrier)

	// Request-reply: отвечаем напрямую отправителю.
	if reply != "" {
		outMsg := nats.Msg{
			Subject: reply,
			Header:  outHeaders,
			Data:    data,
		}
		if err := nc.PublishMsg(&outMsg); err != nil {
			log.Error("ошибка публикации ошибки в reply",
				"reply", reply, "error", err)
		}
	}

	// Публикация в очередь tasks.error.
	outMsg := nats.Msg{
		Subject: errorSubject,
		Header:  outHeaders,
		Data:    data,
	}
	if err := nc.PublishMsg(&outMsg); err != nil {
		log.Error("ошибка публикации в очередь ошибок",
			"subject", errorSubject, "error", err)
	}

	log.Error("задача завершилась ошибкой", "error", errMsg)
}
