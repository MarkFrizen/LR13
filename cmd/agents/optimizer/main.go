package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL      = "nats://localhost:4222"
	subscribeSubject    = "tasks.process"
	publishSubject      = "tasks.completed"
	errorSubject        = "tasks.error"
	auctionSubject      = "auction.tasks"
	taskTypeOptimizer   = "optimizer"
	maxBudgetChangePct  = 20.0
	maxActiveTasks      = 5     // максимум одновременных задач
	refuseCost          = 999999.0  // стоимость отказа (игнорируется аукционером)
)

// Task — входящая задача из очереди.
type Task struct {
	TaskID         string      `json:"task_id"`
	Type           string      `json:"type"`
	TargetAgentID  string      `json:"target_agent_id,omitempty"`
	Complexity     int         `json:"complexity,omitempty"`
	Payload        interface{} `json:"payload,omitempty"`
}

// OptimizerResult — результат оптимизации.
type OptimizerResult struct {
	TaskID        string  `json:"task_id"`
	Type          string  `json:"type"`
	Status        string  `json:"status"`
	Error         string  `json:"error,omitempty"`
	NewBudget     float64 `json:"new_budget,omitempty"`
	OldBudget     float64 `json:"old_budget,omitempty"`
	ChangePercent float64 `json:"change_percent,omitempty"`
	Adjustment    string  `json:"adjustment,omitempty"`
	AgentID       string  `json:"agent_id,omitempty"`
}

// Bid — ставка агента на аукционе.
type Bid struct {
	TaskID           string  `json:"task_id"`
	AgentID          string  `json:"agent_id"`
	Cost             float64 `json:"cost"`
	BaseCost         float64 `json:"base_cost"`
	ComplexityFactor float64 `json:"complexity_factor"`
}

// agentID — уникальный идентификатор этого экземпляра агента.
var agentID string

// activeTasks — количество одновременно выполняемых задач (atomic).
var activeTasks atomic.Int64

// Параметры стоимости (разные для каждого экземпляра).
var (
	baseCost         float64
	complexityFactor float64
)

func init() {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "optimizer"
	}
	suffix := fmt.Sprintf("%06x", rand.Int31n(0xFFFFFF))
	agentID = fmt.Sprintf("%s-%s", hostname, suffix)

	// Каждый агент имеет случайную базовую стоимость и коэффициент сложности.
	baseCost = 5.0 + rand.Float64()*10.0          // 5.0 .. 15.0
	complexityFactor = 0.5 + rand.Float64()*1.5    // 0.5 .. 2.0
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("запуск агента оптимизации",
		"agent_id", agentID,
		"base_cost", fmt.Sprintf("%.2f", baseCost),
		"complexity_factor", fmt.Sprintf("%.2f", complexityFactor),
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

	// Основная очередь задач.
	taskSub, err := nc.Subscribe(subscribeSubject, func(msg *nats.Msg) {
		handleTask(nc, msg)
	})
	if err != nil {
		slog.Error("ошибка подписки", "subject", subscribeSubject, "error", err)
		os.Exit(1)
	}
	defer taskSub.Unsubscribe()

	// Аукционная очередь — получение запросов на торги.
	auctionSub, err := nc.Subscribe(auctionSubject, func(msg *nats.Msg) {
		handleAuctionTask(nc, msg)
	})
	if err != nil {
		slog.Error("ошибка подписки на аукцион", "subject", auctionSubject, "error", err)
		os.Exit(1)
	}
	defer auctionSub.Unsubscribe()

	slog.Info("агент ожидает задачи",
		"subscribe", subscribeSubject,
		"auction", auctionSubject,
		"task_type", taskTypeOptimizer,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	slog.Info("получен сигнал завершения, остановка агента")
	nc.Drain()
	slog.Info("агент завершил работу")
}

// handleAuctionTask обрабатывает запрос на аукцион.
// Агент вычисляет свою ставку и публикует её в reply-канал.
// Если activeTasks > maxActiveTasks — отказывается (cost = refuseCost).
func handleAuctionTask(nc *nats.Conn, msg *nats.Msg) {
	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		slog.Warn("аукцион: невалидная задача", "error", err)
		return
	}

	currentLoad := activeTasks.Load()
	complexity := task.Complexity
	if complexity <= 0 {
		complexity = 5
	}

	cost := baseCost + float64(complexity)*complexityFactor

	// Отказ при перегрузке.
	if currentLoad > maxActiveTasks {
		slog.Warn("аукцион: отказ из-за перегрузки",
			"task_id", task.TaskID,
			"active_tasks", currentLoad,
			"max", maxActiveTasks,
		)
		cost = refuseCost
	}

	bid := Bid{
		TaskID:           task.TaskID,
		AgentID:          agentID,
		Cost:             cost,
		BaseCost:         baseCost,
		ComplexityFactor: complexityFactor,
	}

	data, err := json.Marshal(bid)
	if err != nil {
		slog.Error("аукцион: ошибка сериализации ставки", "error", err)
		return
	}

	if msg.Reply != "" {
		if err := nc.Publish(msg.Reply, data); err != nil {
			slog.Error("аукцион: ошибка публикации ставки", "error", err)
			return
		}
	}

	slog.Info("аукцион: ставка отправлена",
		"task_id", task.TaskID,
		"agent_id", agentID,
		"cost", fmt.Sprintf("%.2f", cost),
		"active_tasks", currentLoad,
	)
}

// handleTask обрабатывает задачу из tasks.process.
// Если задача содержит target_agent_id — обрабатываем только свою.
func handleTask(nc *nats.Conn, msg *nats.Msg) {
	log := slog.With("subject", msg.Subject)

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Error("ошибка десериализации задачи", "error", err)
		publishError(nc, msg.Reply, "", err.Error())
		return
	}

	log = log.With("task_id", task.TaskID, "type", task.Type)

	// Фильтр по типу.
	if task.Type != taskTypeOptimizer {
		log.Warn("пропуск задачи — неверный тип")
		return
	}

	// Фильтр по целевому агенту (аукцион).
	if task.TargetAgentID != "" && task.TargetAgentID != agentID {
		log.Debug("пропуск задачи — предназначена другому агенту",
			"target", task.TargetAgentID, "self", agentID,
		)
		return
	}

	log.Info("получена задача на оптимизацию")

	// Увеличиваем счётчик активных задач.
	activeTasks.Add(1)
	currentLoad := activeTasks.Load()
	log = log.With("active_tasks", currentLoad)

	// Отложенное уменьшение счётчика.
	defer activeTasks.Add(-1)

	// Симуляция оптимизации бюджета.
	oldBudget := float64(rand.Intn(90000) + 10000)
	change := (rand.Float64()*2 - 1) * maxBudgetChangePct
	newBudget := oldBudget * (1 + change/100)

	result := OptimizerResult{
		TaskID:        task.TaskID,
		Type:          task.Type,
		Status:        "completed",
		OldBudget:     oldBudget,
		NewBudget:     newBudget,
		ChangePercent: change,
		AgentID:       agentID,
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
		"adjustment", result.Adjustment,
		"agent_id", result.AgentID,
	)
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
