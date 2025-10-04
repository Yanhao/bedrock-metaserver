package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// IsLeaderFunc is a function type for checking if the current node is the leader
type IsLeaderFunc func() bool

// leaderChecker is a global variable for checking if the current node is the leader
var leaderChecker IsLeaderFunc

// SetLeaderChecker sets the leader checking function
func SetLeaderChecker(checker IsLeaderFunc) {
	leaderChecker = checker
}

// TaskScheduler defines the task scheduler interface
type TaskScheduler interface {
	// SubmitTask submits a task to the scheduler
	SubmitTask(task Task) error
	// Start starts the scheduler
	Start() error
	// Stop stops the scheduler
	Stop() error
	// WaitForTask waits for a specific task to complete
	WaitForTask(taskID string) error
	// AllowSubmission allows task submission
	AllowSubmission()
	// DisallowSubmission disallows task submission
	DisallowSubmission()
	// StopAllTasks stops all running tasks
	StopAllTasks()
}

// taskScheduler is the concrete implementation of the scheduler

type taskScheduler struct {
	// taskQueue is a priority queue for tasks
	taskQueue *TaskPriorityQueue
	// queueMutex protects the taskQueue from concurrent access
	queueMutex sync.Mutex
	// taskDone maps task IDs to their completion status
	taskDone map[string]bool
	// taskErrors maps task IDs to any errors that occurred
	taskErrors map[string]error
	// taskDoneCond is a condition variable for notifying task completion
	taskDoneCond *sync.Cond
	// dataServerLocks ensures sequential execution of operations on each data server
	dataServerLocks sync.Map
	// ctx controls the scheduler's lifecycle
	ctx context.Context
	// cancel is used to stop the scheduler
	cancel context.CancelFunc
	// workerCount is the number of worker goroutines
	workerCount int
	// workerWg is a wait group for worker goroutines
	workerWg sync.WaitGroup
	// running indicates if the scheduler is running
	running bool
	// runningMutex protects the running flag
	runningMutex sync.RWMutex
	// runningTasks tracks running tasks to prevent duplicate submission
	runningTasks sync.Map
	// runningReasons tracks running task reasons to prevent duplicate submission of tasks with the same reason
	runningReasons sync.Map
	// taskReasons maps task IDs to their reasons for cleanup on completion
	taskReasons sync.Map
	// allowSubmission indicates if the scheduler allows task submission
	allowSubmission bool
	// submissionMutex protects the allowSubmission flag
	submissionMutex sync.RWMutex
}

// NewTaskScheduler creates a new task scheduler
func NewTaskScheduler(workerCount int) TaskScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	queue := &TaskPriorityQueue{}
	heap.Init(queue)

	scheduler := &taskScheduler{
		taskQueue:       queue,
		taskDone:        make(map[string]bool),
		taskErrors:      make(map[string]error),
		taskDoneCond:    sync.NewCond(&sync.Mutex{}),
		dataServerLocks: sync.Map{},
		ctx:             ctx,
		cancel:          cancel,
		workerCount:     workerCount,
		running:         false,
		allowSubmission: false, // Default to not allowing task submission
	}

	return scheduler
}

// SubmitTask submits a task to the scheduler
// Ensures only the leader node can submit tasks, prevents duplicate tasks, and prevents duplicate submission of tasks with the same reason
func (s *taskScheduler) SubmitTask(task Task) error {
	if task == nil {
		return nil
	}

	// Check if task submission is allowed
	s.submissionMutex.RLock()
	if !s.allowSubmission {
		s.submissionMutex.RUnlock()
		log.Debug("Task submission is not allowed")
		return nil
	}
	s.submissionMutex.RUnlock()

	s.runningMutex.RLock()
	if !s.running {
		s.runningMutex.RUnlock()
		return nil
	}
	s.runningMutex.RUnlock()

	// Check if task is already running to prevent duplicate submission
	taskID := task.ID()
	if _, exists := s.runningTasks.Load(taskID); exists {
		log.Debugf("Task %s is already running, skip submitting", taskID)
		return nil
	}

	// Check if a task with the same reason is already running
	taskReason := task.Reason()
	if taskReason != "" {
		if _, exists := s.runningReasons.Load(taskReason); exists {
			log.Debugf("Task with reason '%s' is already running, skip submitting", taskReason)
			return nil
		}
		// Record the task reason
		s.runningReasons.Store(taskReason, true)
		// Save the mapping between task ID and reason for cleanup on completion
		s.taskReasons.Store(taskID, taskReason)
	}

	// Mark the task as running
	s.runningTasks.Store(taskID, true)

	// Add task to the priority queue with lock protection
	s.queueMutex.Lock()
	heap.Push(s.taskQueue, task)
	s.queueMutex.Unlock()

	return nil
}

// Start starts the scheduler
func (s *taskScheduler) Start() error {
	s.runningMutex.Lock()
	if s.running {
		s.runningMutex.Unlock()
		return nil
	}
	s.running = true
	s.runningMutex.Unlock()

	// Start worker goroutines
	for i := 0; i < s.workerCount; i++ {
		s.workerWg.Add(1)
		go s.workerLoop()
	}

	log.Info("task scheduler started")
	return nil
}

// Stop stops the scheduler
func (s *taskScheduler) Stop() error {
	s.runningMutex.Lock()
	if !s.running {
		s.runningMutex.Unlock()
		return nil
	}
	s.running = false
	s.runningMutex.Unlock()

	// Cancel the context
	s.cancel()

	// Wait for all worker goroutines to exit
	s.workerWg.Wait()

	log.Info("task scheduler stopped")
	return nil
}

// AllowSubmission allows task submission
func (s *taskScheduler) AllowSubmission() {
	s.submissionMutex.Lock()
	s.allowSubmission = true
	s.submissionMutex.Unlock()
}

// DisallowSubmission disallows task submission
func (s *taskScheduler) DisallowSubmission() {
	s.submissionMutex.Lock()
	s.allowSubmission = false
	s.submissionMutex.Unlock()
}

// StopAllTasks stops all running tasks by clearing the task queue and waiting for running tasks to complete
// Note: This will not cancel tasks that are already running, but it will prevent new tasks from being added to the queue
func (s *taskScheduler) StopAllTasks() {
	// Clear the task queue with lock protection
	s.queueMutex.Lock()
	for s.taskQueue.Len() > 0 {
		_ = heap.Pop(s.taskQueue)
	}
	s.queueMutex.Unlock()

	// Disallow new task submission
	s.DisallowSubmission()

	log.Info("All pending tasks have been cleared, and new task submission is disallowed")
}

// WaitForTask waits for a specific task to complete
func (s *taskScheduler) WaitForTask(taskID string) error {
	s.taskDoneCond.L.Lock()
	defer s.taskDoneCond.L.Unlock()

	// Wait for task completion
	for {
		done, exists := s.taskDone[taskID]
		if exists && done {
			// Task is completed, return any error
			return s.taskErrors[taskID]
		}

		// Wait for condition variable notification
		s.taskDoneCond.Wait()
	}
}

// workerLoop is the main loop for worker goroutines
func (s *taskScheduler) workerLoop() {
	defer s.workerWg.Done()

	for {
		select {
		case <-s.ctx.Done():
			// Context is canceled, exit the loop
			return
		default:
			// Try to get a task from the queue
			task := s.getNextTask()
			if task == nil {
				// Queue is empty, wait for a short time
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// Execute the task
			s.executeTask(task)
		}
	}
}

// getNextTask gets the next task from the queue with lock protection
func (s *taskScheduler) getNextTask() Task {
	s.queueMutex.Lock()
	defer s.queueMutex.Unlock()

	if s.taskQueue.Len() == 0 {
		return nil
	}

	// Pop the task with the highest priority from the priority queue
	task := heap.Pop(s.taskQueue).(Task)
	return task
}

// executeTask executes a task
func (s *taskScheduler) executeTask(task Task) {
	// If it's a SyncTask, execute it directly
	if syncTask, ok := task.(*SyncTask); ok {
		// For sync tasks, ensure sequential execution of operations on each data server
		s.executeSyncTask(syncTask)
		return
	}

	// For async tasks, execute directly
	err := task.Execute(s.ctx)

	// Update task status
	s.updateTaskStatus(task.ID(), err)
}

// executeSyncTask executes a sync task
func (s *taskScheduler) executeSyncTask(task *SyncTask) {
	// For sync tasks, we need to ensure sequential execution of operations on each data server
	// Since sync tasks already execute operations in order internally, we just need to acquire locks for the relevant data servers
	// First, collect all data servers involved in the task
	dataServers := make(map[string]bool)
	for _, op := range task.operations {
		ds := op.GetDataServer()
		if ds != "" {
			dataServers[ds] = true
		}
	}

	// Acquire locks for all data servers
	locks := make([]*sync.Mutex, 0, len(dataServers))
	for ds := range dataServers {
		lock := s.getOrCreateDataServerLock(ds)
		lock.Lock()
		locks = append(locks, lock)
	}

	// Ensure all locks are released when the function exits
	defer func() {
		for _, lock := range locks {
			lock.Unlock()
		}
	}()

	// Execute the task
	err := task.Execute(s.ctx)

	// Update task status
	s.updateTaskStatus(task.ID(), err)
}

// getOrCreateDataServerLock gets or creates a lock for a data server
func (s *taskScheduler) getOrCreateDataServerLock(dataServer string) *sync.Mutex {
	lock, _ := s.dataServerLocks.LoadOrStore(dataServer, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// updateTaskStatus updates the status of a task
func (s *taskScheduler) updateTaskStatus(taskID string, err error) {
	s.taskDoneCond.L.Lock()
	defer s.taskDoneCond.L.Unlock()

	// Update task status
	s.taskDone[taskID] = true
	if err != nil {
		s.taskErrors[taskID] = err
	}

	// Notify waiting goroutines
	s.taskDoneCond.Broadcast()

	// After task completion, remove it from running tasks
	s.runningTasks.Delete(taskID)

	// Clean up task reason records
	if reason, exists := s.taskReasons.Load(taskID); exists {
		if reasonStr, ok := reason.(string); ok && reasonStr != "" {
			s.runningReasons.Delete(reasonStr)
		}
		s.taskReasons.Delete(taskID)
	}
}

// TaskPriorityQueue implements a priority queue for tasks

type TaskPriorityQueue []Task

// Len returns the length of the queue
func (pq TaskPriorityQueue) Len() int {
	return len(pq)
}

// Less compares the priorities of two tasks
func (pq TaskPriorityQueue) Less(i, j int) bool {
	// Tasks with higher priority come first
	return pq[i].Priority() > pq[j].Priority()
}

// Swap swaps the positions of two tasks
func (pq TaskPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a task to the queue
func (pq *TaskPriorityQueue) Push(x interface{}) {
	task := x.(Task)
	*pq = append(*pq, task)
}

// Pop removes and returns the task with the highest priority
func (pq *TaskPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	*pq = old[0 : n-1]
	return task
}

var (
	// globalScheduler is the global scheduler instance
	globalScheduler TaskScheduler
	// schedulerOnce ensures the global scheduler is initialized only once
	schedulerOnce sync.Once
)

// GetTaskScheduler gets the global scheduler instance
func GetTaskScheduler() TaskScheduler {
	schedulerOnce.Do(func() {
		// Create a scheduler with 4 worker goroutines by default
		globalScheduler = NewTaskScheduler(4)
		// Start the scheduler
		globalScheduler.Start()
	})

	return globalScheduler
}
