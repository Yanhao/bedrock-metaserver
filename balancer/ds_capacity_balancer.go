package balancer

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/operation"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

type DsCapacityBalancer struct {
	stop chan struct{}
}

func NewDsCapacityBalancer() *DsCapacityBalancer {
	return &DsCapacityBalancer{
		stop: make(chan struct{}),
	}
}

var (
	dsCapacityBalancer     *DsCapacityBalancer
	dsCapacityBalancerOnce sync.Once
)

func GetDsCapacityBalancer() *DsCapacityBalancer {
	dsCapacityBalancerOnce.Do(func() {
		dsCapacityBalancer = NewDsCapacityBalancer()
	})

	return dsCapacityBalancer
}

func (rb *DsCapacityBalancer) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				rb.doRebalanceByCapacity()

			case <-rb.stop:
				break out
			}
		}

		log.Info("dataserver capacity balancer stopped ...")
	}()

	return nil
}

func (rb *DsCapacityBalancer) Stop() {
	close(rb.stop)
	rb.stop = make(chan struct{})
}

func (rb *DsCapacityBalancer) doRebalanceByCapacity() {
	dsSet := manager.GetDataServerManager().GetDataServersCopy()

	if len(dsSet) == 0 {
		return
	}

	var dsSlice []*model.DataServer
	for _, ds := range dsSet {
		dsSlice = append(dsSlice, ds)
	}

	comparator := func(a, b *model.DataServer) int {
		if a.FreeCapacity > b.FreeCapacity {
			return 1
		} else if a.FreeCapacity < b.FreeCapacity {
			return -1

		}
		return 0
	}
	maxFreeCapacityDs := slices.MaxFunc(dsSlice, comparator)
	minFreeCapaciryDs := slices.MinFunc(dsSlice, comparator)

	if maxFreeCapacityDs.FreeCapacity-minFreeCapaciryDs.FreeCapacity <
		uint64(config.GetConfig().Scheduler.DataserverSpaceBalanceThreshold) {
		return
	}

	shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(minFreeCapaciryDs.Addr())
	if err != nil || len(shardIDs) == 0 {
		return
	}

	shardID2Migrate := shardIDs[rand.Intn(len(shardIDs))]

	shard, err := manager.GetShardManager().GetShard(shardID2Migrate)
	if err != nil {
		return
	}

	// Create migration task and execute via scheduler
	taskID := "balance-migrate-" + time.Now().Format("20060102150405") + "-" + string(rand.Intn(1000))
	task := scheduler.NewBaseTask(taskID, scheduler.PriorityMedium, fmt.Sprintf("balance-migrate-shard-%d", shardID2Migrate))

	// Create shard on target data server
	createOp := operation.NewCreateShardOperation(maxFreeCapacityDs.Addr(), uint64(shardID2Migrate), shard.RangeKeyStart, shard.RangeKeyEnd, 5)
	task.AddOperation(createOp)

	// Migrate shard data from source to target data server
	migrateOp := operation.NewMigrateShardOperation(minFreeCapaciryDs.Addr(), uint64(shardID2Migrate), uint64(shardID2Migrate), maxFreeCapacityDs.Addr(), 5)
	task.AddOperation(migrateOp)

	// Submit task to scheduler
	taskScheduler := scheduler.NewTaskScheduler(5)
	if err := taskScheduler.Start(); err != nil {
		log.Errorf("Failed to start task scheduler: %v", err)
		return
	}
	if err := taskScheduler.SubmitTask(task); err != nil {
		log.Errorf("Failed to submit migration task: %v", err)
		return
	}

	log.Infof("Submitted shard migration task from %s to %s, shard ID: %d",
		minFreeCapaciryDs.Addr(), maxFreeCapacityDs.Addr(), shardID2Migrate)
}
