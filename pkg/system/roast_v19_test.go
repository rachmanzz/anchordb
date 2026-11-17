package system

import (
	"sync"
	"testing"
)

func TestRoastV19_MemoryMonitorConcurrentStopIsSafe(t *testing.T) {
	m := NewMemoryMonitor(100)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Stop()
		}()
	}
	wg.Wait()
}
