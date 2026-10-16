// Package scheduler provides a dependency-free, wall-clock aware background
// task scheduler.
//
// Clock semantics (why "time clock" matters here):
//
//   - Wall clock vs monotonic: a scheduled task fires at a *wall-clock* instant
//     (e.g. "23:00 local time"). Go's time.Time carries a monotonic reading that
//     is immune to NTP/manual clock changes, but calendar math must be done on
//     the wall-clock representation. This scheduler stores the target as a
//     wall-clock instant and re-derives the remaining duration against the
//     current wall clock on every wait slice, so it does not rely on a single
//     monotonic sleep that would fire at the wrong wall time after a clock jump.
//
//   - Timezones & DST: daily schedules are computed with time.Date(...) calendar
//     arithmetic, NOT by adding 24h. Adding 24h shifts the local time by the DST
//     offset (e.g. "23:00" would drift to "00:00" the day after a spring-forward
//     transition). times are resolved in from.Location().
//
//   - Ambiguous / nonexistent local times: during a "fall back" transition the
//     23:00 wall time can be ambiguous; time.Date resolves it deterministically.
//     During a "spring forward" gap a nonexistent target is normalized forward by
//     time.Date. This package never guesses silently beyond that.
package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxWaitChunk bounds a single sleep slice. Waiting in bounded slices lets the
// scheduler notice wall-clock jumps (NTP/manual changes) and stop requests
// within this window, instead of being stuck in one long timer.
const maxWaitChunk = time.Minute

// ScheduledTask defines a background maintenance job contract.
type ScheduledTask interface {
	Schedule() string
	Execute(ctx context.Context) error
}

// TaskInfo represents public serializable task status.
type TaskInfo struct {
	Name        string    `json:"name"`
	Schedule    string    `json:"schedule"`
	LastRun     time.Time `json:"last_run"`
	LastStatus  string    `json:"last_status"`
	NextRun     time.Time `json:"next_run"`
	Running     bool      `json:"running"`
	SkippedRuns int64     `json:"skipped_runs"`
}

// scheduleSpec is the parsed, validated representation of a schedule string.
type scheduleSpec struct {
	cronSpec string
	daily    bool
	hour     int
	min      int
	interval time.Duration
}

type registeredTask struct {
	name        string
	rawSchedule string
	spec        scheduleSpec
	jobFunc     func() error
	task        ScheduledTask

	stop chan struct{} // closed by Unregister to stop this task's loop

	running     atomic.Bool  // single-flight guard (no overlapping executions)
	skippedRuns atomic.Int64 // ticks skipped because the previous run was still active

	mu         sync.RWMutex
	lastRun    time.Time
	lastStatus string
	nextRun    time.Time
}

// nextDaily returns the next occurrence of hour:min strictly after from, using
// calendar arithmetic so DST transitions do not shift the wall-clock time.
func nextDaily(from time.Time, hour, min int) time.Time {
	loc := from.Location()
	y, mo, d := from.Date()
	cand := time.Date(y, mo, d, hour, min, 0, 0, loc)
	if cand.After(from) {
		return cand
	}
	// Advance by one calendar day (handles month/year rollover and DST).
	return time.Date(y, mo, d+1, hour, min, 0, 0, loc)
}

// nextAt computes the next fire instant given a reference instant.
func (t *registeredTask) nextAt(from time.Time) time.Time {
	if t.spec.daily {
		return nextDaily(from, t.spec.hour, t.spec.min)
	}
	if t.spec.interval > 0 {
		return from.Add(t.spec.interval)
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

	log.Printf("[SCHEDULER] Running task %q ...", t.name)
	t0 := time.Now()

	if t.task != nil {
		err = t.task.Execute(ctx)
	} else if t.jobFunc != nil {
		err = t.jobFunc()
	}

	t.mu.Lock()
	if err != nil {
		t.lastStatus = fmt.Sprintf("Error: %v", err)
		log.Printf("[SCHEDULER] Task %q FAILED: %v", t.name, err)
	} else {
		t.lastStatus = "Success"
		log.Printf("[SCHEDULER] Task %q SUCCEEDED in %v", t.name, time.Since(t0))
	}
	t.mu.Unlock()
	return err
}

// runState groups the per-start lifecycle so a restart never races on shared
// fields and WaitGroup.Add is never called concurrently with Wait.
type runState struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   chan struct{}
	wg     sync.WaitGroup
}

// InternalScheduler provides pure Go standard library task scheduling.
type InternalScheduler struct {
	mu    sync.RWMutex
	tasks map[string]*registeredTask
	run   *runState // non-nil only while running

	timeoutNs atomic.Int64 // per-execution timeout (ns); 0 = none
	jitterNs  atomic.Int64 // max random delay added to fire time (ns); 0 = none

	now   func() time.Time                     // injectable clock (defaults to time.Now)
	after func(time.Duration) <-chan time.Time // injectable timer (defaults to time.After)
}

// NewInternalScheduler creates a new scheduler powered by the Go standard library.
func NewInternalScheduler() *InternalScheduler {
	return &InternalScheduler{
		tasks: make(map[string]*registeredTask),
		now:   time.Now,
		after: time.After,
	}
}

// SetTaskTimeout bounds each task execution with a context timeout. 0 disables
// the timeout. This prevents a hung task from blocking graceful shutdown forever.
func (s *InternalScheduler) SetTaskTimeout(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.timeoutNs.Store(int64(d))
}

// TaskTimeout returns the currently configured per-execution timeout (0 if none).
func (s *InternalScheduler) TaskTimeout() time.Duration {
	return time.Duration(s.timeoutNs.Load())
}

// SetJitter adds a random delay in [0, d) to each scheduled fire time to avoid a
// thundering herd when many tasks share the same instant (e.g. all at "00:00").
// 0 disables jitter. The stored NextRun stays the un-jittered schedule.
func (s *InternalScheduler) SetJitter(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.jitterNs.Store(int64(d))
}

// Jitter returns the currently configured fire jitter duration (0 if none).
func (s *InternalScheduler) Jitter() time.Duration {
	return time.Duration(s.jitterNs.Load())
}

// NormalizeSchedule converts convenient formats into standard specifications.
//
// Accepted inputs:
//   - "HH:MM"       -> "M H * * *"   (daily at local wall time)
//   - "<duration>"  -> "@every <d>"  (e.g. "10m", "1h")
//   - "@hourly", "@daily", "@every <d>"
//   - a 5-field cron spec (validated later; only "M H * * *" is executable)
func NormalizeSchedule(schedule string) (string, error) {
	schedule = strings.TrimSpace(schedule)
	if schedule == "" {
		return "", fmt.Errorf("empty schedule")
	}

	// 1. Strict "HH:MM" (rejects trailing garbage like "23:00x").
	if !strings.Contains(schedule, " ") && strings.Contains(schedule, ":") {
		parts := strings.Split(schedule, ":")
		if len(parts) == 2 {
			hour, errH := strconv.Atoi(strings.TrimSpace(parts[0]))
			min, errM := strconv.Atoi(strings.TrimSpace(parts[1]))
			if errH == nil && errM == nil {
				if hour < 0 || hour > 23 || min < 0 || min > 59 {
					return "", fmt.Errorf("invalid time %q: hour must be 0-23 and minute 0-59", schedule)
				}
				return fmt.Sprintf("%d %d * * *", min, hour), nil
			}
		}
	}

	// 2. Raw duration (e.g. "10m", "1h") -> "@every".
	if _, err := time.ParseDuration(schedule); err == nil && !strings.Contains(schedule, " ") {
		return "@every " + schedule, nil
	}

	// 3. Named descriptors (e.g. "@hourly", "@daily", "@every 5m").
	if strings.HasPrefix(schedule, "@") {
		return schedule, nil
	}

	// 4. Standard 5-field cron spec (field-level validation happens in parse).
	if len(strings.Fields(schedule)) == 5 {
		return schedule, nil
	}

	return "", fmt.Errorf("unrecognized schedule format %q", schedule)
}

// parseScheduleConfig validates a schedule and returns its executable form.
// It is strict: schedules it cannot honor are rejected instead of being silently
// mis-scheduled (e.g. a weekly cron must not be treated as daily).
func parseScheduleConfig(schedule string) (scheduleSpec, error) {
	cronSpec, err := NormalizeSchedule(schedule)
	if err != nil {
		return scheduleSpec{}, err
	}
	spec := scheduleSpec{cronSpec: cronSpec}

	switch {
	case cronSpec == "@hourly":
		spec.interval = time.Hour
		return spec, nil
	case cronSpec == "@daily":
		spec.daily = true
		return spec, nil
	case strings.HasPrefix(cronSpec, "@every "):
		d, perr := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(cronSpec, "@every ")))
		if perr != nil {
			return scheduleSpec{}, fmt.Errorf("invalid @every duration: %w", perr)
		}
		if d <= 0 {
			return scheduleSpec{}, fmt.Errorf("interval must be positive, got %s", d)
		}
		spec.interval = d
		return spec, nil
	case strings.HasPrefix(cronSpec, "@"):
		return scheduleSpec{}, fmt.Errorf("unsupported named schedule %q (supported: @hourly, @daily, @every <dur>)", cronSpec)
	}

	// 5-field cron. Only "minute hour * * *" (i.e. every day at a fixed time) is
	// supported. Reject any day/month/weekday constraint instead of mis-parsing.
	fields := strings.Fields(cronSpec)
	if len(fields) != 5 {
		return scheduleSpec{}, fmt.Errorf("unsupported cron spec %q (expected 5 fields)", cronSpec)
	}
	if fields[2] != "*" || fields[3] != "*" || fields[4] != "*" {
		return scheduleSpec{}, fmt.Errorf("unsupported cron spec %q: only 'minute hour * * *' (daily) is supported; day-of-month, month and day-of-week fields must be '*'", cronSpec)
	}
	minute, err := strconv.Atoi(fields[0])
	if err != nil || minute < 0 || minute > 59 {
		return scheduleSpec{}, fmt.Errorf("invalid minute field %q (expected 0-59)", fields[0])
	}
	hour, err := strconv.Atoi(fields[1])
	if err != nil || hour < 0 || hour > 23 {
		return scheduleSpec{}, fmt.Errorf("invalid hour field %q (expected 0-23)", fields[1])
	}
	spec.daily = true
	spec.min = minute
	spec.hour = hour
	return spec, nil
}

// Register registers a named scheduled job.
func (s *InternalScheduler) Register(name, schedule string, job func() error) error {
	if job == nil {
		return fmt.Errorf("job for %q is nil", name)
	}
	return s.register(name, schedule, &registeredTask{jobFunc: job})
}

// RegisterTask binds a ScheduledTask, using task.Schedule() as its schedule.
func (s *InternalScheduler) RegisterTask(name string, task ScheduledTask) error {
	if task == nil {
		return fmt.Errorf("task %q is nil", name)
	}
	return s.register(name, task.Schedule(), &registeredTask{task: task})
}

func (s *InternalScheduler) register(name, schedule string, t *registeredTask) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("task name must not be empty")
	}
	spec, err := parseScheduleConfig(schedule)
	if err != nil {
		return fmt.Errorf("failed to parse schedule for %q: %w", name, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[name]; exists {
		return fmt.Errorf("task %q already registered (call Unregister first)", name)
	}

	t.name = name
	t.rawSchedule = schedule
	t.spec = spec
	t.stop = make(chan struct{})
	t.nextRun = t.nextAt(s.now())

	s.tasks[name] = t
	if s.run != nil {
		s.run.wg.Add(1) // safe: s.run is read/set under s.mu and Wait happens after run=nil
		go s.runTaskLoop(s.run, t)
	}
	log.Printf("[SCHEDULER] Task %q registered with schedule %q (spec: %q)", name, schedule, spec.cronSpec)
	return nil
}

// Unregister removes a task and stops its loop (the current run, if any, is
// allowed to finish). It is safe to re-Register the same name afterwards.
func (s *InternalScheduler) Unregister(name string) error {
	s.mu.Lock()
	t, ok := s.tasks[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("task %q not found", name)
	}
	delete(s.tasks, name)
	s.mu.Unlock()

	close(t.stop)
	log.Printf("[SCHEDULER] Task %q removed.", name)
	return nil
}

// Tasks returns active task metadata and upcoming execution schedules,
// deterministically ordered by name.
func (s *InternalScheduler) Tasks() []TaskInfo {
	s.mu.RLock()
	snapshot := make([]*registeredTask, 0, len(s.tasks))
	for _, t := range s.tasks {
		snapshot = append(snapshot, t)
	}
	s.mu.RUnlock()

	res := make([]TaskInfo, 0, len(snapshot))
	for _, t := range snapshot {
		t.mu.RLock()
		res = append(res, TaskInfo{
			Name:        t.name,
			Schedule:    t.rawSchedule,
			LastRun:     t.lastRun,
			LastStatus:  t.lastStatus,
			NextRun:     t.nextRun,
			Running:     t.running.Load(),
			SkippedRuns: t.skippedRuns.Load(),
		})
		t.mu.RUnlock()
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })
	return res
}

// Start launches the background scheduler goroutines. It is idempotent while
// running; after Stop it can be started again (fresh run state).
func (s *InternalScheduler) Start() {
	s.mu.Lock()
	if s.run != nil {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rs := &runState{ctx: ctx, cancel: cancel, stop: make(chan struct{})}
	s.run = rs
	for _, t := range s.tasks {
		rs.wg.Add(1)
		go s.runTaskLoop(rs, t)
	}
	s.mu.Unlock()

	log.Println("[SCHEDULER] Pure Go background scheduler active.")
}

func (s *InternalScheduler) runTaskLoop(rs *runState, task *registeredTask) {
	defer rs.wg.Done()

	for {
		task.mu.RLock()
		due := task.nextRun
		task.mu.RUnlock()
		if due.IsZero() {
			return
		}

		fireAt := due
		if j := time.Duration(s.jitterNs.Load()); j > 0 {
			// rand.Int63n panics unless its argument is positive, so guard the
			// (extreme but reachable) MaxInt64 jitter that would overflow +1.
			span := int64(j)
			if span == math.MaxInt64 {
				span--
			}
			fireAt = due.Add(time.Duration(rand.Int63n(span + 1)))
		}

		if !s.waitUntil(rs, task, fireAt) {
			return
		}

		if task.running.CompareAndSwap(false, true) {
			if timeout := time.Duration(s.timeoutNs.Load()); timeout > 0 {
				cctx, cancel := context.WithTimeout(rs.ctx, timeout)
				_ = task.execute(cctx)
				cancel()
			} else {
				_ = task.execute(rs.ctx)
			}
			task.running.Store(false)
		} else {
			task.skippedRuns.Add(1)
			log.Printf("[SCHEDULER] Task %q still running; tick skipped to prevent overlap.", task.name)
		}

		// Advance the schedule. Prefer the scheduled grid; if we have fallen
		// behind (long run / missed ticks), skip forward instead of piling up.
		now := s.now()
		next := task.nextAt(due)
		if !next.After(now) {
			log.Printf("[SCHEDULER] Task %q fell behind schedule; missed tick skipped.", task.name)
			next = task.nextAt(now)
		}
		task.mu.Lock()
		task.nextRun = next
		task.mu.Unlock()
	}
}

// waitUntil blocks until fireAt, the scheduler stops, or the task is
// unregistered. It sleeps in bounded slices and re-checks the wall clock each
// slice so that clock jumps are respected and stop is responsive.
func (s *InternalScheduler) waitUntil(rs *runState, task *registeredTask, fireAt time.Time) bool {
	for {
		remaining := fireAt.Sub(s.now())
		if remaining <= 0 {
			return true
		}
		if remaining > maxWaitChunk {
			remaining = maxWaitChunk
		}
		select {
		case <-rs.stop:
			return false
		case <-rs.ctx.Done():
			return false
		case <-task.stop:
			return false
		case <-s.after(remaining):
		}
	}
}

// Stop gracefully shuts down the scheduler with a default 5s bound. In-flight
// tasks receive context cancellation; if they ignore it, Stop returns after the
// timeout instead of blocking forever.
func (s *InternalScheduler) Stop() {
	_ = s.ShutdownWithTimeout(5 * time.Second)
}

// ShutdownWithTimeout is Shutdown with an explicit deadline.
func (s *InternalScheduler) ShutdownWithTimeout(d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return s.Shutdown(ctx)
}

// Shutdown stops accepting new runs, cancels in-flight task contexts, and waits
// for all task loops to finish or until ctx is done. It is idempotent and safe
// to call concurrently with Register (no WaitGroup Add/Wait race).
func (s *InternalScheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	rs := s.run
	if rs == nil {
		s.mu.Unlock()
		return nil
	}
	s.run = nil // block any future wg.Add for this run before waiting
	s.mu.Unlock()

	close(rs.stop)
	rs.cancel()

	done := make(chan struct{})
	go func() {
		rs.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[SCHEDULER] Background scheduler stopped (all tasks finished).")
		return nil
	case <-ctx.Done():
		log.Printf("[SCHEDULER] Background scheduler forcibly stopped (timeout: %v).", ctx.Err())
		return ctx.Err()
	}
}

// RunNow manually triggers a task immediately. If the task is already running
// (e.g. triggered by its schedule), it returns an error instead of overlapping.
func (s *InternalScheduler) RunNow(taskName string) error {
	s.mu.RLock()
	task, exists := s.tasks[taskName]
	rs := s.run
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("task %q not found", taskName)
	}
	if !task.running.CompareAndSwap(false, true) {
		return fmt.Errorf("task %q is already running", taskName)
	}
	defer task.running.Store(false)

	base := context.Background()
	if rs != nil {
		base = rs.ctx
	}
	if timeout := time.Duration(s.timeoutNs.Load()); timeout > 0 {
		ctx, cancel := context.WithTimeout(base, timeout)
		defer cancel()
		return task.execute(ctx)
	}
	return task.execute(base)
}

// CalculateNextRun returns the next fire time for a schedule relative to
// fromTime. Daily times are resolved with calendar arithmetic (DST-safe).
func CalculateNextRun(schedule string, fromTime time.Time) (time.Time, error) {
	spec, err := parseScheduleConfig(schedule)
	if err != nil {
		return time.Time{}, err
	}
	if spec.daily {
		return nextDaily(fromTime, spec.hour, spec.min), nil
	}
	if spec.interval > 0 {
		return fromTime.Add(spec.interval), nil
	}
	return time.Time{}, fmt.Errorf("invalid schedule format %q", schedule)
}
