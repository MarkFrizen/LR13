package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL  = "nats://localhost:4222"
	subscribeSubject  = "tasks.process"
	publishSubject    = "tasks.completed"
	taskTypeSegment   = "segment"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("[segment-agent] запуск...")

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = defaultNATSURL
	}

	// Подключение к NATS с переподключением.
	nc, err := nats.Connect(natsURL, nats.ReconnectWait(2*time.Second), nats.MaxReconnects(-1))
	if err != nil {
		log.Fatalf("[segment-agent] не удалось подключиться к NATS: %v", err)
	}
	defer nc.Close()
	log.Printf("[segment-agent] подключён к NATS: %s", natsURL)

	// Подписка на очередь tasks.process.
	sub, err := nc.Subscribe(subscribeSubject, func(msg *nats.Msg) {
		handleTask(msg)
	})
	if err != nil {
		log.Fatalf("[segment-agent] ошибка подписки на %s: %v", subscribeSubject, err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("[segment-agent] ошибка отписки: %v", err)
		}
	}()
	log.Printf("[segment-agent] подписан на %s, ожидание задач типа '%s'...", subscribeSubject, taskTypeSegment)

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[segment-agent] завершение работы...")
}

// handleTask обрабатывает входящее сообщение из NATS.
func handleTask(msg *nats.Msg) {
	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		log.Printf("[segment-agent] ошибка парсинга задачи: %v", err)
		publishError(msg.Reply, "", "невалидный JSON: "+err.Error())
		return
	}

	log.Printf("[segment-agent] получена задача: task_id=%s, type=%s, clients=%d",
		task.TaskID, task.Type, len(task.Clients))

	// Фильтр: обрабатываем только задачи типа "segment".
	if task.Type != taskTypeSegment {
		log.Printf("[segment-agent] пропуск задачи type=%s (ожидался %s)", task.Type, taskTypeSegment)
		return
	}

	if len(task.Clients) == 0 {
		log.Printf("[segment-agent] пустой список клиентов в задаче %s", task.TaskID)
		publishError(task.TaskID, task.Type, "список клиентов пуст")
		return
	}

	// Выполнение сегментации.
	segments := segment(task.Clients)

	result := Result{
		TaskID:   task.TaskID,
		Type:     task.Type,
		Segments: segments,
		Status:   "completed",
	}

	// Публикация результата.
	publishResult(msg, result)
}

// publishResult публикует результат сегментации в tasks.completed.
// Если msg.Reply не пустой, отвечаем напрямую (request-reply),
// иначе публикуем в publishSubject.
func publishResult(msg *nats.Msg, result Result) {
	data, err := json.Marshal(result)
	if err != nil {
		log.Printf("[segment-agent] ошибка сериализации результата: %v", err)
		return
	}

	subject := publishSubject
	if msg.Reply != "" {
		subject = msg.Reply
	}

	if err := msg.Respond(data); err != nil {
		log.Printf("[segment-agent] ошибка публикации ответа в %s: %v", subject, err)
		return
	}

	log.Printf("[segment-agent] результат опубликован в %s: task_id=%s, сегментов=%d",
		subject, result.TaskID, len(result.Segments))
	for _, s := range result.Segments {
		log.Printf("  -> %s: %d клиентов", s.Name, s.Count)
	}
}

// publishError публикует результат с ошибкой.
func publishError(taskID, taskType, errMsg string) {
	// При ошибке нет ссылки на msg, логируем только локально.
	log.Printf("[segment-agent] ОШИБКА task_id=%s: %s", taskID, errMsg)
}
