package main

// Client представляет данные клиента из входящей задачи.
type Client struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Age       int    `json:"age"`
	Region    string `json:"region"`
	Purchases int    `json:"purchases"`
}

// Task — сообщение из очереди tasks.process.
type Task struct {
	TaskID  string   `json:"task_id"`
	Type    string   `json:"type"`
	Clients []Client `json:"clients"`
}

// Segment — одна группа сегментированной аудитории.
type Segment struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Clients     []Client `json:"clients"`
	Count       int      `json:"count"`
}

// Result публикуется в очередь tasks.completed.
type Result struct {
	TaskID   string    `json:"task_id"`
	Type     string    `json:"type"`
	Segments []Segment `json:"segments"`
	Status   string    `json:"status"`
	Error    string    `json:"error,omitempty"`
}
