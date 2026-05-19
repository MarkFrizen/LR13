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

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Error("ошибка десериализации задачи", "error", err)
		publishError(nc, msg.Reply, "", "", "невалидный JSON: "+err.Error())
		return
	}

	log = log.With("task_id", task.TaskID, "type", task.Type, "clients", len(task.Clients))
	log.Info("получена задача на сегментацию")

	// Фильтр: обрабатываем только задачи типа "segment".
	if task.Type != taskTypeSegment {
		log.Warn("пропуск задачи — неверный тип")
		publishError(nc, msg.Reply, task.TaskID, task.Type,
			"ожидался тип 'segment', получен '"+task.Type+"'")
		return
	}

	if len(task.Clients) == 0 {
		log.Error("пустой список клиентов")
		publishError(nc, msg.Reply, task.TaskID, task.Type,
			"список клиентов пуст")
		return
	}

	// Выполнение сегментации.
	segments := segment(task.Clients)
	log.Info("сегментация завершена", "segments", len(segments))

	result := Result{
		TaskID:   task.TaskID,
		Type:     task.Type,
		Segments: segments,
		Status:   "completed",
	}

	publishResult(nc, msg, result, log)
}

// publishResult публикует успешный результат сегментации.
// Если msg.Reply не пустой — используем request-reply;
// иначе публикуем в tasks.completed.
func publishResult(nc *nats.Conn, msg *nats.Msg, result Result, log *slog.Logger) {
	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации результата", "error", err)
		return
	}

	// Request-reply: отвечаем напрямую отправителю.
	if msg.Reply != "" {
		if err := msg.Respond(data); err != nil {
			log.Error("ошибка respond", "reply", msg.Reply, "error", err)
		}
		log.Info("результат отправлен через request-reply",
			"reply", msg.Reply, "segments", len(result.Segments))
		return
	}

	// Публикация в очередь tasks.completed.
	if err := nc.Publish(publishSubject, data); err != nil {
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

// publishError публикует ошибку обработки задачи.
// Если задан reply-канал — отвечаем через request-reply.
// В любом случае дублируем ошибку в очередь tasks.error.
func publishError(nc *nats.Conn, reply, taskID, taskType, errMsg string) {
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

	// Request-reply: отвечаем напрямую отправителю.
	if reply != "" {
		if err := nc.Publish(reply, data); err != nil {
			log.Error("ошибка публикации ошибки в reply",
				"reply", reply, "error", err)
		}
	}

	// Публикация в очередь tasks.error.
	if err := nc.Publish(errorSubject, data); err != nil {
		log.Error("ошибка публикации в очередь ошибок",
			"subject", errorSubject, "error", err)
	}

	log.Error("задача завершилась ошибкой", "error", errMsg)
}
