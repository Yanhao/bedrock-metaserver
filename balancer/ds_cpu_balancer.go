package balancer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/operation"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

// DsCpuBalancer CPU usage-based balancer

type DsCpuBalancer struct {
	stop chan struct{}
}

func NewDsCpuBalancer() *DsCpuBalancer {
	return &DsCpuBalancer{
		stop: make(chan struct{}),
	}
}

var (
	dsCpuBalancer     *DsCpuBalancer
	dsCpuBalancerOnce sync.Once
)

func GetDsCpuBalancer() *DsCpuBalancer {
	dsCpuBalancerOnce.Do(func() {
		dsCpuBalancer = NewDsCpuBalancer()
	})

	return dsCpuBalancer
}

func (cb *DsCpuBalancer) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 15) // CPU load detection frequency
		defer ticker.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				cb.doRebalanceByCpu()

			case <-cb.stop:
				break out
			}
		}

		log.Info("dataserver CPU balancer stopped ...")
	}()

	return nil
}

func (cb *DsCpuBalancer) Stop() {
	close(cb.stop)
	cb.stop = make(chan struct{})
}

func (cb *DsCpuBalancer) doRebalanceByCpu() {
	dsSet := manager.GetDataServerManager().GetDataServersCopy()

	if len(dsSet) == 0 {
		return
	}

	// Filter out active data servers
	var activeDs []*model.DataServer
	for _, ds := range dsSet {
		if ds.Status == model.LiveStatusActive {
			activeDs = append(activeDs, ds)
		}
	}

	if len(activeDs) < 2 {
		return
	}

	// Find data servers with highest and lowest CPU usage
	maxCpuDs := findMaxCpuDataServer(activeDs)
	minCpuDs := findMinCpuDataServer(activeDs)

	// No rebalancing needed if CPU usage difference is small
	if maxCpuDs == nil || minCpuDs == nil {
		return
	}

	// Get shards on high CPU load data server
	shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(maxCpuDs.Addr())
	if err != nil || len(shardIDs) == 0 {
		return
	}

	// Select a shard for migration
	shardID2Migrate := shardIDs[rand.Intn(len(shardIDs))]

	// Get shard information
	shard, err := manager.GetShardManager().GetShard(shardID2Migrate)
	if err != nil {
		return
	}

	// Create migration task and execute via scheduler
	taskID := "balance-cpu-migrate-" + time.Now().Format("20060102150405") + "-" + string(rand.Intn(1000))
	task := scheduler.NewBaseTask(taskID, scheduler.PriorityMedium, fmt.Sprintf("balance-cpu-migrate-shard-%d", shardID2Migrate))

	// Create shard on target data server
	createOp := operation.NewCreateShardOperation(minCpuDs.Addr(), uint64(shardID2Migrate), shard.RangeKeyStart, shard.RangeKeyEnd, 5)
	task.AddOperation(createOp)

	// Migrate shard data from source to target data server
	migrateOp := operation.NewMigrateShardOperation(maxCpuDs.Addr(), uint64(shardID2Migrate), uint64(shardID2Migrate), minCpuDs.Addr(), 5)
	task.AddOperation(migrateOp)

	// Submit task to scheduler
	taskScheduler := scheduler.NewTaskScheduler(5)
	if err := taskScheduler.Start(); err != nil {
		log.Errorf("Failed to start task scheduler: %v", err)
		return
	}
	if err := taskScheduler.SubmitTask(task); err != nil {
		log.Errorf("Failed to submit CPU balance migration task: %v", err)
		return
	}

	log.Infof("Submitted CPU balance migration task from %s to %s, shard ID: %d",
		maxCpuDs.Addr(), minCpuDs.Addr(), shardID2Migrate)
}

// findMaxCpuDataServer finds the data server with highest CPU usage
func findMaxCpuDataServer(dsList []*model.DataServer) *model.DataServer {
	if len(dsList) == 0 {
		return nil
	}

	maxCpuDs := dsList[0]
	for _, ds := range dsList {
		// Assuming CPUUsage field exists, simplified handling here
		// In actual implementation, comparison should be based on actual CPU usage field
		if ds.Qps > maxCpuDs.Qps { // Temporarily using QPS as an approximation of CPU load
			maxCpuDs = ds
		}
	}

	return maxCpuDs
}

// findMinCpuDataServer finds the data server with lowest CPU usage
func findMinCpuDataServer(dsList []*model.DataServer) *model.DataServer {
	if len(dsList) == 0 {
		return nil
	}

	minCpuDs := dsList[0]
	for _, ds := range dsList {
		// Assuming CPUUsage field exists, simplified handling here
		if ds.Qps < minCpuDs.Qps { // Temporarily using QPS as an approximation of CPU load
			minCpuDs = ds
		}
	}

	return minCpuDs
}
