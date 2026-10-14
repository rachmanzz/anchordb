package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	gsuite_cron "github.com/rachmanzz/gosuite/v3/cron"
	robfig "github.com/robfig/cron/v3"
)

// ScheduledTask defines a background maintenance job
type ScheduledTask struct {
	Name       string
	Schedule   string // e.g. "23:00" (daily at 23:00) or "@hourly" or "10m"
	JobFunc    func() error
	LastRun    time.Time
	LastStatus string
	NextRun    time.Time
}

// TaskInfo represents public serializable task status
type TaskInfo struct {
	Name       string    `json:"name"`
	Schedule   string    `json:"schedule"`
	LastRun    time.Time `json:"last_run"`
	LastStatus string    `json:"last_status"`
	NextRun    time.Time `json:"next_run"`
}

// cronTaskWrapper bridges AnchorDB tasks to gosuite/v3/cron.ScheduledTask
type cronTaskWrapper struct {
	name        string
	rawSchedule string
	cronSpec    string
	jobFunc     func() error
	entryID     gsuite_cron.EntryID

	mu         sync.RWMutex
	lastRun    time.Time
	lastStatus string
}

func (w *cronTaskWrapper) Schedule() string {
	return w.cronSpec
}

func (w *cronTaskWrapper) Execute(ctx context.Context) error {
	w.mu.Lock()
	w.lastRun = time.Now()
	w.mu.Unlock()

	log.Printf("[SCHEDULER] Menjalankan task %q ...", w.name)
	t0 := time.Now()
	err := w.jobFunc()

	w.mu.Lock()
	if err != nil {
		w.lastStatus = fmt.Sprintf("Error: %v", err)
		log.Printf("[SCHEDULER] Task %q GAGAL: %v", w.name, err)
	} else {
		w.lastStatus = "Success"
		log.Printf("[SCHEDULER] Task %q SUKSES dalam %v", w.name, time.Since(t0))
	}
	w.mu.Unlock()
	return err
}

func (w *cronTaskWrapper) OnError(ctx context.Context, err error) {
	log.Printf("[SCHEDULER] Task %q recoverable error: %v", w.name, err)
}

func (w *cronTaskWrapper) OnFail(ctx context.Context, err error) {
	log.Printf("[SCHEDULER] Task %q critical failure: %v", w.name, err)
}

// InternalScheduler provides production-grade scheduling using github.com/rachmanzz/gosuite/v3/cron
type InternalScheduler struct {
	mu        sync.RWMutex
	cronSched *gsuite_cron.Scheduler
	tasks     map[string]*cronTaskWrapper
	running   bool
}

// NewInternalScheduler creates a new scheduler powered by gosuite/v3/cron
func NewInternalScheduler() *InternalScheduler {
	return &InternalScheduler{
		cronSched: gsuite_cron.NewScheduler(),
		tasks:     make(map[string]*cronTaskWrapper),
	}
}

// Tasks returns active task metadata and upcoming execution schedules
func (s *InternalScheduler) Tasks() []TaskInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.cronSched.Entries()
	entryMap := make(map[robfig.EntryID]robfig.Entry, len(entries))
	for _, e := range entries {
		entryMap[e.ID] = e
	}

	var res []TaskInfo
	for _, t := range s.tasks {
		t.mu.RLock()
		lastRun := t.lastRun
		lastStatus := t.lastStatus
		t.mu.RUnlock()

		nextRun := time.Time{}
		if entry, ok := entryMap[t.entryID]; ok {
			nextRun = entry.Next
			if lastRun.IsZero() && !entry.Prev.IsZero() {
				lastRun = entry.Prev
			}
		}

		res = append(res, TaskInfo{
			Name:       t.name,
			Schedule:   t.rawSchedule,
			LastRun:    lastRun,
			LastStatus: lastStatus,
			NextRun:    nextRun,
		})
	}
	return res
}

// NormalizeSchedule converts convenient formats ("23:00", "10m") into robfig/cron compatible specs
func NormalizeSchedule(schedule string) (string, error) {
	schedule = strings.TrimSpace(schedule)
	if schedule == "" {
		return "", fmt.Errorf("empty schedule")
	}

	// 1. HH:MM format (e.g. "23:00" or "02:30")
	var hour, min int
	if n, _ := fmt.Sscanf(schedule, "%d:%d", &hour, &min); n == 2 && !strings.Contains(schedule, " ") {
		if hour < 0 || hour > 23 || min < 0 || min > 59 {
			return "", fmt.Errorf("invalid time %q: hour must be 0-23 and minute 0-59", schedule)
		}
		return gsuite_cron.EveryDay(schedule), nil
	}

	// 2. Duration format (e.g. "10m", "1h", "24h")
	if _, err := time.ParseDuration(schedule); err == nil && !strings.Contains(schedule, " ") {
		return "@every " + schedule, nil
	}

	// 3. Named descriptors (e.g. "@hourly", "@daily", "@every 5m")
	if strings.HasPrefix(schedule, "@") {
		return schedule, nil
	}

	// 4. Standard 5-field cron spec (e.g. "0 23 * * *")
	fields := strings.Fields(schedule)
	if len(fields) == 5 {
		return schedule, nil
	}

	return "", fmt.Errorf("unrecognized schedule format %q (use HH:MM like '23:00', duration like '1h', or 5-field cron spec)", schedule)
}

// Register registers a named scheduled job
func (s *InternalScheduler) Register(name, schedule string, job func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cronSpec, err := NormalizeSchedule(schedule)
	if err != nil {
		return fmt.Errorf("failed to parse schedule for %q: %w", name, err)
	}

	wrapper := &cronTaskWrapper{
		name:        name,
		rawSchedule: schedule,
		cronSpec:    cronSpec,
		jobFunc:     job,
	}

	// Register with gosuite cron scheduler using AddJob to get exact EntryID
	runner := gsuite_cron.NewTaskRunner(wrapper)
	entryID, err := s.cronSched.AddJob(cronSpec, runner)
	if err != nil {
		return fmt.Errorf("failed to register cron task %q: %w", name, err)
	}
	wrapper.entryID = entryID

	s.tasks[name] = wrapper
	log.Printf("[SCHEDULER] Task %q terdaftar dengan jadwal %q (spec: %q, entry: %d)", name, schedule, cronSpec, entryID)
	return nil
}

// Start launches the background scheduler
func (s *InternalScheduler) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	s.cronSched.Start()
	log.Println("[SCHEDULER] Background scheduler aktif.")
}

// Stop gracefully shuts down the background scheduler with a timeout
func (s *InternalScheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.cronSched.Stop(ctx); err != nil {
		log.Printf("[SCHEDULER] Stop warning: %v", err)
	}
	log.Println("[SCHEDULER] Background scheduler dihentikan.")
}

// RunNow manually triggers a task immediately
func (s *InternalScheduler) RunNow(taskName string) error {
	s.mu.RLock()
	task, exists := s.tasks[taskName]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %q not found", taskName)
	}

	return task.Execute(context.Background())
}

// CalculateNextRun parses "HH:MM" (e.g. "23:00") or duration ("1h", "30m") for backward compatibility
func CalculateNextRun(schedule string, fromTime time.Time) (time.Time, error) {
	var hour, min int
	if n, _ := fmt.Sscanf(schedule, "%d:%d", &hour, &min); n == 2 {
		targetToday := time.Date(
			fromTime.Year(), fromTime.Month(), fromTime.Day(),
			hour, min, 0, 0, fromTime.Location(),
		)
		if targetToday.After(fromTime) {
			return targetToday, nil
		}
		return targetToday.Add(24 * time.Hour), nil
	}

	if d, err := time.ParseDuration(schedule); err == nil {
		return fromTime.Add(d), nil
	}

	return time.Time{}, fmt.Errorf("invalid schedule format %q", schedule)
}
