package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL   = "nats://localhost:4222"
	subscribeSubject = "tasks.process"
	publishSubject   = "tasks.completed"
	errorSubject     = "tasks.error"
	taskTypeCampaign = "campaign"
	maxMessagesPerHour = 1000
)

type Task struct {
	TaskID  string        `json:"task_id"`
	Type    string        `json:"type"`
	Clients []interface{} `json:"clients,omitempty"`
	Payload interface{}   `json:"payload,omitempty"`
}

type CampaignResult struct {
	TaskID    string `json:"task_id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Sent      int    `json:"sent,omitempty"`
	Failed    int    `json:"failed,omitempty"`
	Channel   string `json:"channel,omitempty"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("запуск агента рассылок")

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
		slog.Error("ошибка подписки", "subject", subscribeSubject, "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	slog.Info("агент ожидает задачи",
		"subscribe", subscribeSubject,
		"task_type", taskTypeCampaign,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	slog.Info("получен сигнал завершения, остановка агента")
	nc.Drain()
	slog.Info("агент завершил работу")
}

func handleTask(nc *nats.Conn, msg *nats.Msg) {
	log := slog.With("subject", msg.Subject)

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Error("ошибка десериализации задачи", "error", err)
		publishError(nc, msg.Reply, "", err.Error())
		return
	}

	log = log.With("task_id", task.TaskID, "type", task.Type)

	if task.Type != taskTypeCampaign {
		log.Warn("пропуск задачи — неверный тип")
		return
	}

	log.Info("получена задача на рассылку")

	// Симуляция рассылки: случайное число отправленных/ошибок.
	sent := rand.Intn(500) + 100
	failed := rand.Intn(20)

	// Симуляция лимита: если превышен, часть не отправлена.
	if sent+failed > maxMessagesPerHour {
		overflow := sent + failed - maxMessagesPerHour
		sent -= overflow
		if sent < 0 {
			sent = 0
		}
		failed = 0
		log.Warn("превышен лимит рассылки", "max_per_hour", maxMessagesPerHour)
	}

	result := CampaignResult{
		TaskID:  task.TaskID,
		Type:    task.Type,
		Status:  "completed",
		Sent:    sent,
		Failed:  failed,
		Channel: "email",
	}

	publishResult(nc, msg, result, log)
}

func publishResult(nc *nats.Conn, msg *nats.Msg, result CampaignResult, log *slog.Logger) {
	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации результата", "error", err)
		return
	}

	if msg.Reply != "" {
		if err := msg.Respond(data); err != nil {
			log.Error("ошибка respond", "reply", msg.Reply, "error", err)
		}
		log.Info("результат отправлен через request-reply", "reply", msg.Reply)
		return
	}

	if err := nc.Publish(publishSubject, data); err != nil {
		log.Error("ошибка публикации результата", "subject", publishSubject, "error", err)
		return
	}

	log.Info("рассылка выполнена",
		"sent", result.Sent, "failed", result.Failed, "channel", result.Channel)
}

func publishError(nc *nats.Conn, reply, taskID, errMsg string) {
	log := slog.With("task_id", taskID)

	result := CampaignResult{
		TaskID: taskID,
		Type:   taskTypeCampaign,
		Status: "error",
		Error:  errMsg,
	}

	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации ошибки", "error", err)
		return
	}

	if reply != "" {
		nc.Publish(reply, data)
	}
	nc.Publish(errorSubject, data)

	log.Error("задача завершилась ошибкой", "error", errMsg)
}
