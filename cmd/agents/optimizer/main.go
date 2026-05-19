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
	defaultNATSURL    = "nats://localhost:4222"
	subscribeSubject  = "tasks.process"
	publishSubject    = "tasks.completed"
	errorSubject      = "tasks.error"
	taskTypeOptimizer = "optimizer"
	maxBudgetChangePct = 20.0
)

type Task struct {
	TaskID  string        `json:"task_id"`
	Type    string        `json:"type"`
	Payload interface{}   `json:"payload,omitempty"`
}

type OptimizerResult struct {
	TaskID        string  `json:"task_id"`
	Type          string  `json:"type"`
	Status        string  `json:"status"`
	Error         string  `json:"error,omitempty"`
	NewBudget     float64 `json:"new_budget,omitempty"`
	OldBudget     float64 `json:"old_budget,omitempty"`
	ChangePercent float64 `json:"change_percent,omitempty"`
	Adjustment    string  `json:"adjustment,omitempty"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("запуск агента оптимизации")

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
		"task_type", taskTypeOptimizer,
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

	if task.Type != taskTypeOptimizer {
		log.Warn("пропуск задачи — неверный тип")
		return
	}

	log.Info("получена задача на оптимизацию")

	// Симуляция оптимизации бюджета.
	oldBudget := float64(rand.Intn(90000) + 10000)
	change := (rand.Float64()*2 - 1) * maxBudgetChangePct // -20% .. +20%
	newBudget := oldBudget * (1 + change/100)

	result := OptimizerResult{
		TaskID:        task.TaskID,
		Type:          task.Type,
		Status:        "completed",
		OldBudget:     oldBudget,
		NewBudget:     newBudget,
		ChangePercent: change,
	}

	if change > 0 {
		result.Adjustment = "increase"
	} else {
		result.Adjustment = "decrease"
	}

	publishResult(nc, msg, result, log)
}

func publishResult(nc *nats.Conn, msg *nats.Msg, result OptimizerResult, log *slog.Logger) {
	data, err := json.Marshal(result)
	if err != nil {
		log.Error("ошибка сериализации результата", "error", err)
		return
	}

	if msg.Reply != "" {
		if err := msg.Respond(data); err != nil {
			log.Error("ошибка respond", "reply", msg.Reply, "error", err)
		}
		return
	}

	if err := nc.Publish(publishSubject, data); err != nil {
		log.Error("ошибка публикации результата", "subject", publishSubject, "error", err)
		return
	}

	log.Info("оптимизация завершена",
		"old_budget", result.OldBudget,
		"new_budget", result.NewBudget,
		"change_pct", result.ChangePercent,
		"adjustment", result.Adjustment)
}

func publishError(nc *nats.Conn, reply, taskID, errMsg string) {
	log := slog.With("task_id", taskID)

	result := OptimizerResult{
		TaskID: taskID,
		Type:   taskTypeOptimizer,
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
