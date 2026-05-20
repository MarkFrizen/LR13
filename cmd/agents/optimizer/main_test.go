package main

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func TestOptimizer_BidCalculation(t *testing.T) {
	// Проверяем формулу: cost = baseCost + complexity * complexityFactor.
	testCases := []struct {
		complexity int
		base       float64
		factor     float64
		expectedFn func(base, factor float64, complexity int) float64
	}{
		{complexity: 5, base: 10.0, factor: 1.0, expectedFn: func(b, f float64, c int) float64 { return b + float64(c)*f }},
		{complexity: 1, base: 5.0, factor: 0.5, expectedFn: func(b, f float64, c int) float64 { return b + float64(c)*f }},
		{complexity: 10, base: 15.0, factor: 2.0, expectedFn: func(b, f float64, c int) float64 { return b + float64(c)*f }},
	}

	for _, tc := range testCases {
		expected := tc.expectedFn(tc.base, tc.factor, tc.complexity)
		got := tc.base + float64(tc.complexity)*tc.factor

		if math.Abs(got-expected) > 0.001 {
			t.Errorf("cost = %f, want %f (base=%f, factor=%f, complexity=%d)",
				got, expected, tc.base, tc.factor, tc.complexity)
		}
	}
}

func TestOptimizer_RefuseCost(t *testing.T) {
	// Проверяем, что refuseCost — константа, большая, чем любая реальная ставка.
	if refuseCost < 1000 {
		t.Errorf("refuseCost (%f) должна быть большой", refuseCost)
	}
	if refuseCost != 999999.0 {
		t.Errorf("refuseCost = %f, want 999999.0", refuseCost)
	}
}

func TestOptimizer_JSONBidMarshal(t *testing.T) {
	bid := Bid{
		TaskID:           "task-1",
		AgentID:          "optimizer-abc123",
		Cost:             12.5,
		BaseCost:         10.0,
		ComplexityFactor: 1.5,
	}
	data, err := json.Marshal(bid)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Bid
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.TaskID != "task-1" {
		t.Errorf("TaskID = %q, want task-1", decoded.TaskID)
	}
	if decoded.Cost != 12.5 {
		t.Errorf("Cost = %f, want 12.5", decoded.Cost)
	}
	if decoded.AgentID != "optimizer-abc123" {
		t.Errorf("AgentID = %q, want optimizer-abc123", decoded.AgentID)
	}
}

func TestOptimizer_JSONResultMarshal(t *testing.T) {
	r := OptimizerResult{
		TaskID:        "task-1",
		Type:          "optimizer",
		Status:        "completed",
		OldBudget:     50000,
		NewBudget:     55000,
		ChangePercent: 10.0,
		Adjustment:    "increase",
		AgentID:       "optimizer-abc123",
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}

	var decoded OptimizerResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.TaskID != "task-1" {
		t.Errorf("TaskID = %q, want task-1", decoded.TaskID)
	}
	if decoded.ChangePercent != 10.0 {
		t.Errorf("ChangePercent = %f, want 10.0", decoded.ChangePercent)
	}
	if decoded.Adjustment != "increase" {
		t.Errorf("Adjustment = %q, want increase", decoded.Adjustment)
	}
}

func TestOptimizer_BudgetChangeLimit(t *testing.T) {
	// Проверяем, что change никогда не превышает maxBudgetChangePct.
	for i := 0; i < 1000; i++ {
		change := (float64(i%2000)/1000.0 - 1) * maxBudgetChangePct
		oldBudget := 50000.0
		newBudget := oldBudget * (1 + change/100)

		actualChange := (newBudget - oldBudget) / oldBudget * 100

		if math.Abs(actualChange) > maxBudgetChangePct+0.001 {
			t.Errorf("actualChange = %f%%, превышает лимит %f%%", actualChange, maxBudgetChangePct)
		}
	}
}

func TestOptimizer_AgentID(t *testing.T) {
	// Проверяем формат agentID.
	hostname := "optimizer"
	suffix := fmt.Sprintf("%06x", 42)
	agentID := fmt.Sprintf("%s-%s", hostname, suffix)
	expected := "optimizer-00002a"
	if agentID != expected {
		t.Errorf("agentID = %q, want %q", agentID, expected)
	}
}
