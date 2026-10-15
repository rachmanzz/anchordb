package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ScheduledTask defines a background maintenance job contract
type ScheduledTask interface {
	Schedule() string
	Execute(ctx context.Context) error
}

// TaskInfo represents public serializable task status
type TaskInfo struct {
	Name       string    `json:"name"`
	Schedule   string    `json:"schedule"`
	LastRun    time.Time `json:"last_run"`
	LastStatus string    `json:"last_status"`
	NextRun    time.Time `json:"next_run"`
}

type registeredTask struct {
	name        string
	rawSchedule string
	cronSpec    string
	jobFunc     func() error
	task        ScheduledTask

	isDaily    bool
	targetHour int
	targetMin  int
	interval   time.Duration

	mu         sync.RWMutex
	lastRun    time.Time
	lastStatus string
	nextRun    time.Time
}

func (t *registeredTask) calculateNext(from time.Time) time.Time {
	if t.isDaily {
		targetToday := time.Date(from.Year(), from.Month(), from.Day(), t.targetHour, t.targetMin, 0, 0, from.Location())
		if targetToday.After(from) {
			return targetToday
		}
		return targetToday.Add(24 * time.Hour)
	}
	if t.interval > 0 {
		return from.Add(t.interval)
	}
	return time.Time{}
}

func (t *registeredTask) execute(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in task: %v", r)
			t.mu.Lock()
			t.lastStatus = fmt.Sprintf("Panic: %v", r)
			t.mu.Unlock()
			log.Printf("[SCHEDULER] Task %q PANIC: %v", t.name, r)
		}
	}()

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	log.Printf("[SCHEDULER] Menjalankan task %q ...", t.name)
	t0 := time.Now()

	if t.task != nil {
		err = t.task.Execute(ctx)
	} else if t.jobFunc != nil {
		err = t.jobFunc()
	}

	t.mu.Lock()
	if err != nil {
		t.lastStatus = fmt.Sprintf("Error: %v", err)
		log.Printf("[SCHEDULER] Task %q GAGAL: %v", t.name, err)
	} else {
		t.lastStatus = "Success"
		log.Printf("[SCHEDULER] Task %q SUKSES dalam %v", t.name, time.Since(t0))
	}
	t.mu.Unlock()
	return err
}

// InternalScheduler provides pure Go standard library task scheduling
type InternalScheduler struct {
	mu      sync.RWMutex
	tasks   map[string]*registeredTask
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewInternalScheduler creates a new scheduler powered by Go standard library
func NewInternalScheduler() *InternalScheduler {
	return &InternalScheduler{
		tasks:  make(map[string]*registeredTask),
		stopCh: make(chan struct{}),
	}
}

// NormalizeSchedule converts convenient formats into standard specifications
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
		return fmt.Sprintf("%d %d * * *", min, hour), nil
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

	return "", fmt.Errorf("unrecognized schedule format %q", schedule)
}

func parseScheduleConfig(schedule string) (isDaily bool, targetHour, targetMin int, interval time.Duration, cronSpec string, err error) {
	cronSpec, err = NormalizeSchedule(schedule)
	if err != nil {
		return false, 0, 0, 0, "", err
	}

	// Check if HH:MM
	var h, m int
	if n, _ := fmt.Sscanf(schedule, "%d:%d", &h, &m); n == 2 && !strings.Contains(schedule, " ") {
		return true, h, m, 0, cronSpec, nil
	}

	// Check 5-field "M H * * *"
	var minField, hourField int
	if n, _ := fmt.Sscanf(cronSpec, "%d %d * * *", &minField, &hourField); n == 2 {
		return true, hourField, minField, 0, cronSpec, nil
	}

	// Check @hourly / @daily
	if cronSpec == "@hourly" {
		return false, 0, 0, time.Hour, cronSpec, nil
	}
	if cronSpec == "@daily" {
		return true, 0, 0, 0, cronSpec, nil
	}

	// Check @every duration or raw duration
	durStr := strings.TrimPrefix(cronSpec, "@every ")
	if d, parseErr := time.ParseDuration(strings.TrimSpace(durStr)); parseErr == nil {
		return false, 0, 0, d, cronSpec, nil
	}

	return false, 0, 0, 0, cronSpec, fmt.Errorf("unsupported schedule execution: %s", schedule)
}

// Register registers a named scheduled job
func (s *InternalScheduler) Register(name, schedule string, job func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	isDaily, h, m, interval, cronSpec, err := parseScheduleConfig(schedule)
	if err != nil {
		return fmt.Errorf("failed to parse schedule for %q: %w", name, err)
	}

	task := &registeredTask{
		name:        name,
		rawSchedule: schedule,
		cronSpec:    cronSpec,
		jobFunc:     job,
		isDaily:     isDaily,
		targetHour:  h,
		targetMin:   m,
		interval:    interval,
		nextRun:     time.Now(),
	}
	task.nextRun = task.calculateNext(time.Now())

	s.tasks[name] = task
	if s.running {
		s.wg.Add(1)
		go s.runTaskLoop(task)
	}
	log.Printf("[SCHEDULER] Task %q terdaftar dengan jadwal %q (spec: %q)", name, schedule, cronSpec)
	return nil
}

// RegisterTask binds a ScheduledTask
func (s *InternalScheduler) RegisterTask(name string, task ScheduledTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rawSchedule := task.Schedule()
	isDaily, h, m, interval, cronSpec, err := parseScheduleConfig(rawSchedule)
	if err != nil {
		return fmt.Errorf("failed to parse schedule for %q: %w", name, err)
	}

	regTask := &registeredTask{
		name:        name,
		rawSchedule: rawSchedule,
		cronSpec:    cronSpec,
		task:        task,
		isDaily:     isDaily,
		targetHour:  h,
		targetMin:   m,
		interval:    interval,
		nextRun:     time.Now(),
	}
	regTask.nextRun = regTask.calculateNext(time.Now())

	s.tasks[name] = regTask
	if s.running {
		s.wg.Add(1)
		go s.runTaskLoop(regTask)
	}
	log.Printf("[SCHEDULER] Task %q terdaftar dengan jadwal %q (spec: %q)", name, rawSchedule, cronSpec)
	return nil
}

// Tasks returns active task metadata and upcoming execution schedules
func (s *InternalScheduler) Tasks() []TaskInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]TaskInfo, 0, len(s.tasks))
	for _, t := range s.tasks {
		t.mu.RLock()
		res = append(res, TaskInfo{
			Name:       t.name,
			Schedule:   t.rawSchedule,
			LastRun:    t.lastRun,
			LastStatus: t.lastStatus,
			NextRun:    t.nextRun,
		})
		t.mu.RUnlock()
	}
	return res
}

// Start launches the background scheduler goroutines
func (s *InternalScheduler) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	s.mu.RLock()
	for _, task := range s.tasks {
		s.wg.Add(1)
		go s.runTaskLoop(task)
	}
	s.mu.RUnlock()

	log.Println("[SCHEDULER] Pure Go background scheduler aktif.")
}

func (s *InternalScheduler) runTaskLoop(task *registeredTask) {
	defer s.wg.Done()

	for {
		task.mu.RLock()
		next := task.nextRun
		task.mu.RUnlock()

		now := time.Now()
		sleepDur := next.Sub(now)
		if sleepDur <= 0 {
			sleepDur = 10 * time.Millisecond
		}

		select {
		case <-s.stopCh:
			return
		case <-time.After(sleepDur):
			_ = task.execute(context.Background())
			task.mu.Lock()
			task.nextRun = task.calculateNext(time.Now())
			task.mu.Unlock()
		}
	}
}

// Stop gracefully shuts down the background scheduler
func (s *InternalScheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
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

	return task.execute(context.Background())
}

// CalculateNextRun parses "HH:MM" (e.g. "23:00") or duration ("1h", "30m")
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
