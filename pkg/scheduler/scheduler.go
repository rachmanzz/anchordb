package scheduler

import (
	"fmt"
	"log"
	"sync"
	"time"
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

// InternalScheduler provides daemon scheduling inside AnchorDB without external cron
type InternalScheduler struct {
	mu      sync.RWMutex
	tasks   map[string]*ScheduledTask
	stopCh  chan struct{}
	running bool
}

func NewInternalScheduler() *InternalScheduler {
	return &InternalScheduler{
		tasks:  make(map[string]*ScheduledTask),
		stopCh: make(chan struct{}),
	}
}

// TaskInfo represents public serializable task status
type TaskInfo struct {
	Name       string    `json:"name"`
	Schedule   string    `json:"schedule"`
	LastRun    time.Time `json:"last_run"`
	LastStatus string    `json:"last_status"`
	NextRun    time.Time `json:"next_run"`
}

func (s *InternalScheduler) Tasks() []TaskInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var res []TaskInfo
	for _, t := range s.tasks {
		res = append(res, TaskInfo{
			Name:       t.Name,
			Schedule:   t.Schedule,
			LastRun:    t.LastRun,
			LastStatus: t.LastStatus,
			NextRun:    t.NextRun,
		})
	}
	return res
}

// Register adds a scheduled job (e.g. Schedule="23:00" for daily 23:00)
func (s *InternalScheduler) Register(name, schedule string, job func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextRun, err := CalculateNextRun(schedule, time.Now())
	if err != nil {
		return err
	}

	s.tasks[name] = &ScheduledTask{
		Name:     name,
		Schedule: schedule,
		JobFunc:  job,
		NextRun:  nextRun,
	}
	log.Printf("[SCHEDULER] Task %q terdaftar dengan jadwal %q (Jadwal berikutnya: %s)",
		name, schedule, nextRun.Format("2006-01-02 15:04:05"))
	return nil
}

// Start launches the background scheduler loop
func (s *InternalScheduler) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	go s.loop()
}

// Stop gracefully shuts down the background scheduler
func (s *InternalScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}
	close(s.stopCh)
	s.running = false
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

	return s.executeTask(task)
}

func (s *InternalScheduler) loop() {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			for _, task := range s.tasks {
				if now.After(task.NextRun) || now.Equal(task.NextRun) {
					go func(t *ScheduledTask) {
						_ = s.executeTask(t)
					}(task)

					// Calculate next run
					next, err := CalculateNextRun(task.Schedule, now)
					if err == nil {
						task.NextRun = next
					}
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *InternalScheduler) executeTask(t *ScheduledTask) error {
	log.Printf("[SCHEDULER] Menjalankan task %q ...", t.Name)
	t0 := time.Now()
	err := t.JobFunc()

	s.mu.Lock()
	t.LastRun = t0
	if err != nil {
		t.LastStatus = fmt.Sprintf("Error: %v", err)
		log.Printf("[SCHEDULER] Task %q GAGAL: %v", t.Name, err)
	} else {
		t.LastStatus = "Success"
		log.Printf("[SCHEDULER] Task %q SUKSES dalam %v", t.Name, time.Since(t0))
	}
	s.mu.Unlock()
	return err
}

// CalculateNextRun parses "HH:MM" (e.g. "23:00") or duration ("1h", "30m")
func CalculateNextRun(schedule string, fromTime time.Time) (time.Time, error) {
	// Format HH:MM (e.g. "23:00")
	var hour, min int
	if n, _ := fmt.Sscanf(schedule, "%d:%d", &hour, &min); n == 2 {
		targetToday := time.Date(
			fromTime.Year(), fromTime.Month(), fromTime.Day(),
			hour, min, 0, 0, fromTime.Location(),
		)
		if targetToday.After(fromTime) {
			return targetToday, nil
		}
		// If 23:00 already passed today, schedule for 23:00 tomorrow
		return targetToday.Add(24 * time.Hour), nil
	}

	// Format Duration (e.g. "1h", "10m", "24h")
	if d, err := time.ParseDuration(schedule); err == nil {
		return fromTime.Add(d), nil
	}

	return time.Time{}, fmt.Errorf("invalid schedule format %q (use HH:MM like '23:00' or duration like '1h')", schedule)
}
