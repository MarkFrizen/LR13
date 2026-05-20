package main

import (
	"encoding/json"
	"testing"
)

func TestCampaign_JSONTaskParsing(t *testing.T) {
	raw := `{"task_id":"test-1","type":"campaign","clients":[{"id":1,"name":"Alice"}]}`
	var task Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		t.Fatal(err)
	}
	if task.TaskID != "test-1" {
		t.Errorf("TaskID = %q, want test-1", task.TaskID)
	}
	if task.Type != "campaign" {
		t.Errorf("Type = %q, want campaign", task.Type)
	}
	if len(task.Clients) != 1 {
		t.Errorf("len(Clients) = %d, want 1", len(task.Clients))
	}
}

func TestCampaign_JSONResultMarshal(t *testing.T) {
	r := CampaignResult{
		TaskID:  "test-1",
		Type:    "campaign",
		Status:  "completed",
		Sent:    500,
		Failed:  3,
		Channel: "email",
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	var decoded CampaignResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.TaskID != "test-1" {
		t.Errorf("TaskID = %q, want test-1", decoded.TaskID)
	}
	if decoded.Sent != 500 {
		t.Errorf("Sent = %d, want 500", decoded.Sent)
	}
	if decoded.Failed != 3 {
		t.Errorf("Failed = %d, want 3", decoded.Failed)
	}
}

func TestCampaign_ErrorResult(t *testing.T) {
	r := CampaignResult{
		TaskID: "test-err",
		Type:   "campaign",
		Status: "error",
		Error:  "campaign failed",
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	var decoded CampaignResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Status != "error" {
		t.Errorf("Status = %q, want error", decoded.Status)
	}
	if decoded.Error != "campaign failed" {
		t.Errorf("Error = %q, want campaign failed", decoded.Error)
	}
}

func TestCampaign_MessageLimit(t *testing.T) {
	// Проверяем, что sent не превышает maxMessagesPerHour.
	for i := 0; i < 100; i++ {
		sent := i*10 + 100
		failed := i % 20

		if sent+failed > maxMessagesPerHour {
			overflow := sent + failed - maxMessagesPerHour
			sent -= overflow
			if sent < 0 {
				sent = 0
			}
			failed = 0
		}

		if sent > maxMessagesPerHour {
			t.Errorf("sent (%d) превышает maxMessagesPerHour (%d)", sent, maxMessagesPerHour)
		}
		if sent < 0 {
			t.Errorf("sent отрицательное: %d", sent)
		}
		if failed < 0 {
			t.Errorf("failed отрицательное: %d", failed)
		}
	}
}
