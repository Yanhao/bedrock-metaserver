package scheduler

import (
	"sync"
)

// Initialization function to ensure the global scheduler is properly initialized when the package is loaded
func init() {
	// Pre-initialize the global scheduler
	// Note: This only ensures the global scheduler is initialized but not started immediately
	// The actual start will happen when GetTaskScheduler() is called for the first time
	var once sync.Once
	once.Do(func() {
		// No specific initialization here, initialization logic is in GetTaskScheduler()
	})
}

// Shutdown closes the scheduler and cleans up resources
func Shutdown() error {
	if globalScheduler != nil {
		return globalScheduler.Stop()
	}
	return nil
}
