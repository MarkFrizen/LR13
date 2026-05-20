package main

import (
	"encoding/json"
	"testing"
)

func TestAnalytics_JSONTaskParsing(t *testing.T) {
	raw := `{"task_id":"test-1","type":"analytics","payload":{"campaign":"email"}}`
	var task Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		t.Fatal(err)
	}
	if task.TaskID != "test-1" {
		t.Errorf("TaskID = %q, want test-1", task.TaskID)
	}
	if task.Type != "analytics" {
		t.Errorf("Type = %q, want analytics", task.Type)
	}
}

func TestAnalytics_JSONResultMarshal(t *testing.T) {
	r := AnalyticsResult{
		TaskID:      "test-1",
		Type:        "analytics",
		Status:      "completed",
		CTR:         12.5,
		ROI:         45.3,
		Opens:       500,
		Clicks:      100,
		Conversions: 10,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	var decoded AnalyticsResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.TaskID != "test-1" {
		t.Errorf("TaskID = %q, want test-1", decoded.TaskID)
	}
	if decoded.CTR != 12.5 {
		t.Errorf("CTR = %f, want 12.5", decoded.CTR)
	}
	if decoded.ROI != 45.3 {
		t.Errorf("ROI = %f, want 45.3", decoded.ROI)
	}
}

func TestAnalytics_ErrorResult(t *testing.T) {
	r := AnalyticsResult{
		TaskID: "test-err",
		Type:   "analytics",
		Status: "error",
		Error:  "something went wrong",
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	var decoded AnalyticsResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Status != "error" {
		t.Errorf("Status = %q, want error", decoded.Status)
	}
	if decoded.Error != "something went wrong" {
		t.Errorf("Error = %q, want something went wrong", decoded.Error)
	}
}

func TestAnalytics_ReasonableMetricBounds(t *testing.T) {
	// Проверяем, что метрики находятся в разумных границах.
	testCases := []AnalyticsResult{
		{TaskID: "t1", Type: "analytics", Status: "completed", CTR: 0, ROI: 0, Opens: 200, Clicks: 0, Conversions: 0},
		{TaskID: "t2", Type: "analytics", Status: "completed", CTR: 25.0, ROI: 15.5, Opens: 400, Clicks: 100, Conversions: 10},
		{TaskID: "t3", Type: "analytics", Status: "completed", CTR: 50.0, ROI: 29.9, Opens: 1000, Clicks: 500, Conversions: 50},
		{TaskID: "t4", Type: "analytics", Status: "completed", CTR: 12.3, ROI: 7.8, Opens: 750, Clicks: 92, Conversions: 8},
	}

	for _, result := range testCases {
		if result.Opens < 200 || result.Opens > 1000 {
			t.Errorf("Opens вне диапазона: %d", result.Opens)
		}
		if result.CTR < 0 || result.CTR > 100 {
			t.Errorf("CTR вне диапазона: %f", result.CTR)
		}
		if result.ROI < 0 || result.ROI > 30 {
			t.Errorf("ROI вне диапазона: %f", result.ROI)
		}
		if result.Clicks > result.Opens {
			t.Errorf("Clicks (%d) > Opens (%d)", result.Clicks, result.Opens)
		}
		if result.Conversions > result.Clicks {
			t.Errorf("Conversions (%d) > Clicks (%d)", result.Conversions, result.Clicks)
		}
		if result.Conversions < 0 {
			t.Errorf("Conversions отрицательное: %d", result.Conversions)
		}
	}
}
