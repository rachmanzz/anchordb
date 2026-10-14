package scheduler

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalizeSchedule(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		hasError bool
	}{
		{"23:00", "0 23 * * *", false},
		{"02:30", "30 2 * * *", false},
		{"10m", "@every 10m", false},
		{"1h", "@every 1h", false},
		{"@hourly", "@hourly", false},
		{"@daily", "@daily", false},
		{"0 0 * * *", "0 0 * * *", false},
		{"invalid format", "", true},
		{"", "", true},
		{"25:00", "", true},
	}

	for _, tt := range tests {
		got, err := NormalizeSchedule(tt.input)
		if tt.hasError {
			if err == nil {
				t.Errorf("NormalizeSchedule(%q) expected error, got nil (result: %q)", tt.input, got)
			}
		} else {
			if err != nil {
				t.Errorf("NormalizeSchedule(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.expected {
				t.Errorf("NormalizeSchedule(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		}
	}
}

func TestInternalScheduler(t *testing.T) {
	sched := NewInternalScheduler()
	defer sched.Stop()

	var counter int32
	err := sched.Register("test_task", "@every 1s", func() error {
		atomic.AddInt32(&counter, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	tasks := sched.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Name != "test_task" {
		t.Fatalf("expected task name 'test_task', got %s", tasks[0].Name)
	}

	// Test RunNow
	if err := sched.RunNow("test_task"); err != nil {
		t.Fatalf("RunNow failed: %v", err)
	}
	if atomic.LoadInt32(&counter) != 1 {
		t.Fatalf("counter expected 1 after RunNow, got %d", atomic.LoadInt32(&counter))
	}

	// Test Tasks reflect success
	tasks = sched.Tasks()
	if tasks[0].LastStatus != "Success" {
		t.Errorf("expected LastStatus 'Success', got %q", tasks[0].LastStatus)
	}

	// Test Failing Task
	_ = sched.Register("failing_task", "@every 1h", func() error {
		return errors.New("simulated error")
	})
	_ = sched.RunNow("failing_task")
	tasks = sched.Tasks()
	var failingFound bool
	for _, task := range tasks {
		if task.Name == "failing_task" {
			failingFound = true
			if task.LastStatus != "Error: simulated error" {
				t.Errorf("expected failing status, got %q", task.LastStatus)
			}
		}
	}
	if !failingFound {
		t.Errorf("failing_task not found in tasks")
	}

	// Test start and stop
	sched.Start()
	time.Sleep(100 * time.Millisecond)
	sched.Stop()
}
