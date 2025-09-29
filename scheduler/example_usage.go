package scheduler

import (
	"fmt"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/operation"
)

// This file provides examples of how to use the task scheduler

// ExampleTaskSchedulerUsage demonstrates how to use the task scheduler
func ExampleTaskSchedulerUsage() {
	// Get global scheduler instance
	scheduler := GetTaskScheduler()

	// Create a synchronous task with high priority, add task reason
	syncTask := NewSyncTask("sync-task-1", PriorityHigh, "example-sync-operation")

	// Create some operations and add to the task
	createOp := operation.NewCreateShardOperation(
		"127.0.0.1:8001", // Data server address
		1001,             // Shard ID
		[]byte{0x0},      // Min key
		[]byte{0xFF},     // Max key
		10,               // Operation priority
	)
	syncTask.AddOperation(createOp)

	splitOp := operation.NewSplitShardOperation(
		"127.0.0.1:8001", // Data server address
		1001,             // Original shard ID
		1002,             // New shard ID
		10,               // Operation priority
	)
	syncTask.AddOperation(splitOp)

	// Submit task to scheduler
	scheduler.SubmitTask(syncTask)

	// Wait for task completion
	scheduler.WaitForTask(syncTask.ID())

	// Create an async task with medium priority, with task reason
	asyncTask := NewAsyncTask("async-task-1", PriorityMedium, "example-async-operation")

	// Create some operations and add to the async task
	migrateOp1 := operation.NewMigrateShardOperation(
		"127.0.0.1:8001", // Source data server address
		1001,             // Source shard ID
		1003,             // Target shard ID
		"127.0.0.1:8002", // Target data server address
		5,                // Operation priority
	)
	asyncTask.AddOperation(migrateOp1)

	migrateOp2 := operation.NewMigrateShardOperation(
		"127.0.0.1:8001", // Source data server address
		1002,             // Source shard ID
		1004,             // Target shard ID
		"127.0.0.1:8002", // Target data server address
		5,                // Operation priority
	)
	asyncTask.AddOperation(migrateOp2)

	// Submit task to scheduler
	scheduler.SubmitTask(asyncTask)

	// Async tasks don't need to wait for completion, but can if needed
	// scheduler.WaitForTask(asyncTask.ID())

	// Output task execution status
	fmt.Printf("Sync task %s done\n", syncTask.ID())
	fmt.Printf("Async task %s submitted\n", asyncTask.ID())
}

// ExampleAllocatorUsingScheduler demonstrates how the allocator uses the scheduler to execute tasks
func ExampleAllocatorUsingScheduler() {
	// This is an example showing how the allocator uses the scheduler
	// In actual implementation, the allocator creates tasks and operations as needed

	// Get global scheduler instance
	scheduler := GetTaskScheduler()

	// Create a sync task (allocator-produced tasks need to be sync)
	task := NewSyncTask("allocate-shard-1", PriorityUrgent, "example-allocate-shard")

	// Assume the allocator needs to create a new shard
	createOp := operation.NewCreateShardOperation(
		"127.0.0.1:8001", // Data server address
		1005,             // Shard ID
		[]byte{0x0},      // Min key
		[]byte{0xFF},     // Max key
		100,              // Operation priority
	)
	task.AddOperation(createOp)

	// Add replicate operation to other data servers
	replicateOp1 := operation.NewCreateShardOperation(
		"127.0.0.1:8002", // Data server address
		1005,             // Shard ID
		[]byte{0x0},      // Min key
		[]byte{0xFF},     // Max key
		100,              // Operation priority
	)
	task.AddOperation(replicateOp1)

	replicateOp2 := operation.NewCreateShardOperation(
		"127.0.0.1:8003", // Data server address
		1005,             // Shard ID
		[]byte{0x0},      // Min key
		[]byte{0xFF},     // Max key
		100,              // Operation priority
	)
	task.AddOperation(replicateOp2)

	// Submit task to scheduler
	scheduler.SubmitTask(task)

	// Wait for task completion (allocator needs to wait synchronously for task completion)
	scheduler.WaitForTask(task.ID())

	// Check if task completed successfully
	if err := task.GetError(); err != nil {
		fmt.Printf("Task %s failed: %v\n", task.ID(), err)
	} else {
		fmt.Printf("Task %s succeeded\n", task.ID())
	}
}

// ExampleBalancerUsingScheduler demonstrates how the balancer uses the scheduler to execute tasks
func ExampleBalancerUsingScheduler() {
	// This is an example showing how the balancer uses the scheduler
	// In actual implementation, the balancer creates tasks and operations as needed

	// Get global scheduler instance
	scheduler := GetTaskScheduler()

	// Create an async task (balancer can use async tasks)
	task := NewAsyncTask("balance-shard-1", PriorityMedium, "example-balance-shard")

	// Assume the balancer needs to migrate a shard to balance load
	migrateOp := operation.NewMigrateShardOperation(
		"127.0.0.1:8001", // Source data server address
		1001,             // Source shard ID
		1006,             // Target shard ID
		"127.0.0.1:8004", // Target data server address
		5,                // Operation priority
	)
	task.AddOperation(migrateOp)

	// Submit task to scheduler
	scheduler.SubmitTask(task)

	// Balancer doesn't need to wait for task completion, tasks run asynchronously in the background
	fmt.Printf("Balance task %s submitted\n", task.ID())

	// If needed, you can check task status later
	time.Sleep(5 * time.Second) // Wait for some time
	if task.IsDone() {
		if err := task.GetError(); err != nil {
			fmt.Printf("Task %s failed: %v\n", task.ID(), err)
		} else {
			fmt.Printf("Task %s succeeded\n", task.ID())
		}
	} else {
		fmt.Printf("Task %s is still running\n", task.ID())
	}
}
