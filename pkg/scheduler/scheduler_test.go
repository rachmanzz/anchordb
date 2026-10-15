package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ctxTask is a ScheduledTask whose execution is context-aware, used to verify
// per-task timeouts and cancellation.
type ctxTask struct {
	sched string
	run   func(ctx context.Context) error
}

func (c ctxTask) Schedule() string                  { return c.sched }
func (c ctxTask) Execute(ctx context.Context) error { return c.run(ctx) }

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

// --- Clock / timezone safety -------------------------------------------------

func TestNextDailyDSTSafe(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("zoneinfo unavailable, skipping DST test: %v", err)
	}

	// 2026-03-08 is a US "spring forward" transition (02:00 -> 03:00).
	// The next 23:00 after 2026-03-07 23:30 must be 2026-03-08 23:00 local,
	// NOT 2026-03-09 00:00 (which is what a naive +24h produces).
	from := time.Date(2026, 3, 7, 23, 30, 0, 0, loc)
	got := nextDaily(from, 23, 0)
	want := time.Date(2026, 3, 8, 23, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("nextDaily DST mismatch: got %v, want %v", got, want)
	}

	naive := time.Date(2026, 3, 7, 23, 0, 0, 0, loc).Add(24 * time.Hour)
	if got.Equal(naive) {
		t.Fatalf("expected DST-safe calendar result to differ from naive +24h (%v)", naive)
	}
}

func TestCalculateNextRunDSTSafe(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("zoneinfo unavailable, skipping DST test: %v", err)
	}
	from := time.Date(2026, 11, 1, 0, 30, 0, 0, loc) // fall-back day
	next, err := CalculateNextRun("23:00", from)
	if err != nil {
		t.Fatalf("CalculateNextRun failed: %v", err)
	}
	if next.Hour() != 23 || next.Minute() != 0 {
		t.Fatalf("expected 23:00 wall time, got %v", next)
	}
	if next.Location() != loc {
		t.Fatalf("expected result in %v, got %v", loc, next.Location())
	}
}

// --- Strict schedule validation ---------------------------------------------

func TestParseScheduleConfigStrict(t *testing.T) {
	bad := []string{
		"0 0 * * 1",   // weekly must not be silently treated as daily
		"*/5 * * * *", // step expressions are not supported
		"0 0 1 * *",   // day-of-month constraint
		"0 0 * 1 *",   // month constraint
		"@weekly",     // unsupported named schedule
		"@yearly",     // unsupported named schedule
		"23:00x",      // trailing garbage in time
		"0 24 * * *",  // hour out of range
		"60 0 * * *",  // minute out of range
	}
	for _, b := range bad {
		if _, err := parseScheduleConfig(b); err == nil {
			t.Errorf("parseScheduleConfig(%q) expected error, got nil", b)
		}
	}

	good := []string{"23:00", "02:30", "0 23 * * *", "@hourly", "@daily", "@every 5m", "10m", "1h"}
	for _, g := range good {
		if _, err := parseScheduleConfig(g); err != nil {
			t.Errorf("parseScheduleConfig(%q) unexpected error: %v", g, err)
		}
	}
}

// --- Wait loop: wall-clock jumps and stop responsiveness ---------------------

func TestWaitUntilFiresWhenClockJumpsForward(t *testing.T) {
	s := NewInternalScheduler()
	base := time.Now()
	current := base
	s.now = func() time.Time { return current }

	calls := 0
	// Simulate a wait slice elapsing; after the first slice the wall clock
	// jumps forward past the target (as an NTP correction would).
	s.after = func(d time.Duration) <-chan time.Time {
		calls++
		if calls == 1 {
			current = base.Add(2 * time.Hour)
		}
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}

	rs := &runState{ctx: context.Background(), cancel: func() {}, stop: make(chan struct{})}
	task := &registeredTask{stop: make(chan struct{})}

	if !s.waitUntil(rs, task, base.Add(time.Hour)) {
		t.Fatal("expected waitUntil to fire after the clock jumped past the target")
	}
	if calls == 0 {
		t.Fatal("expected the wait loop to poll at least once")
	}
}

func TestWaitUntilResponsiveToStop(t *testing.T) {
	s := NewInternalScheduler()
	s.after = func(d time.Duration) <-chan time.Time { return make(chan time.Time) } // never fires

	rs := &runState{ctx: context.Background(), cancel: func() {}, stop: make(chan struct{})}
	task := &registeredTask{stop: make(chan struct{})}
	close(rs.stop)

	if s.waitUntil(rs, task, time.Now().Add(time.Hour)) {
		t.Fatal("expected waitUntil to abort when the scheduler is stopped")
	}
}

// --- Concurrency / lifecycle safety ------------------------------------------

func TestRunNowPreventsOverlap(t *testing.T) {
	s := NewInternalScheduler()
	started := make(chan struct{})
	release := make(chan struct{})
	if err := s.Register("block", "@every 1h", func() error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.RunNow("block") }()
	<-started

	if err := s.RunNow("block"); err == nil {
		t.Fatal("expected an overlap error while the task is already running")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first RunNow failed: %v", err)
	}
}

func TestDuplicateRegisterAndUnregister(t *testing.T) {
	s := NewInternalScheduler()
	noop := func() error { return nil }

	if err := s.Register("a", "@every 1h", noop); err != nil {
		t.Fatalf("first Register failed: %v", err)
	}
	if err := s.Register("a", "@every 1h", noop); err == nil {
		t.Fatal("expected duplicate registration to be rejected")
	}
	if err := s.Unregister("a"); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	if err := s.Register("a", "@every 1h", noop); err != nil {
		t.Fatalf("re-register after Unregister failed: %v", err)
	}
}

func TestShutdownIdempotentAndRestart(t *testing.T) {
	s := NewInternalScheduler()
	if err := s.Register("t", "@every 1h", func() error { return nil }); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	s.Start()
	s.Start() // idempotent while running

	if err := s.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatalf("first Shutdown failed: %v", err)
	}
	if err := s.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatalf("second Shutdown should be a no-op, got: %v", err)
	}

	s.Start() // restart must be safe after a clean shutdown
	if err := s.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatalf("Shutdown after restart failed: %v", err)
	}
}

func TestTaskTimeoutCancelsExecution(t *testing.T) {
	s := NewInternalScheduler()
	s.SetTaskTimeout(30 * time.Millisecond)

	var canceled int32
	err := s.RegisterTask("slow", ctxTask{
		sched: "@every 1h",
		run: func(ctx context.Context) error {
			<-ctx.Done()
			atomic.StoreInt32(&canceled, 1)
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("RegisterTask failed: %v", err)
	}

	start := time.Now()
	if err := s.RunNow("slow"); err == nil {
		t.Fatal("expected RunNow to return a timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout not enforced, took %v", elapsed)
	}
	if atomic.LoadInt32(&canceled) != 1 {
		t.Fatal("expected the task context to be canceled by the timeout")
	}
}

func TestTimeoutAndJitterGetters(t *testing.T) {
	s := NewInternalScheduler()
	if s.TaskTimeout() != 0 {
		t.Fatalf("expected 0 default timeout, got %v", s.TaskTimeout())
	}
	if s.Jitter() != 0 {
		t.Fatalf("expected 0 default jitter, got %v", s.Jitter())
	}

	s.SetTaskTimeout(5 * time.Minute)
	s.SetJitter(10 * time.Second)

	if s.TaskTimeout() != 5*time.Minute {
		t.Fatalf("expected 5m, got %v", s.TaskTimeout())
	}
	if s.Jitter() != 10*time.Second {
		t.Fatalf("expected 10s, got %v", s.Jitter())
	}
}
