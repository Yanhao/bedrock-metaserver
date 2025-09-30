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

// HotShardBalancer hot shard balancer

const (
	// HotShardThreshold QPS threshold for hot shards
	HotShardThreshold = 1000
)

type HotShardBalancer struct {
	stop chan struct{}
}

func NewHotShardBalancer() *HotShardBalancer {
	return &HotShardBalancer{
		stop: make(chan struct{}),
	}
}

var (
	hotShardBalancer     *HotShardBalancer
	hotShardBalancerOnce sync.Once
)

func GetHotShardBalancer() *HotShardBalancer {
	hotShardBalancerOnce.Do(func() {
		hotShardBalancer = NewHotShardBalancer()
	})

	return hotShardBalancer
}

func (hb *HotShardBalancer) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 20) // Hot shard detection frequency
		defer ticker.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				hb.doRebalanceHotShards()

			case <-hb.stop:
				break out
			}
		}

		log.Info("hot shard balancer stopped ...")
	}()

	return nil
}

func (hb *HotShardBalancer) Stop() {
	close(hb.stop)
	hb.stop = make(chan struct{})
}

func (hb *HotShardBalancer) doRebalanceHotShards() {
	// Get all data servers
	dsSet := manager.GetDataServerManager().GetDataServersCopy()

	// Collect all hot shards
	var hotShards []model.ShardIDAndQps
	var allShardIDs []model.ShardID

	for _, ds := range dsSet {
		// Check hot shards on data server
		for _, hotShard := range ds.HotShards {
			if hotShard.QPS > HotShardThreshold {
				hotShards = append(hotShards, hotShard)
			}
		}

		// Get all shard IDs on data server
		shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(ds.Addr())
		if err == nil {
			allShardIDs = append(allShardIDs, shardIDs...)
		}
	}

	// No rebalancing needed if there are no hot shards
	if len(hotShards) == 0 {
		return
	}

	// Find suitable migration targets for each hot shard
	for _, hotShard := range hotShards {
		// Find the data server containing the hot shard
		dsWithHotShard := hb.findDataServerWithShard(hotShard.ID, dsSet)
		if dsWithHotShard == nil {
			continue
		}

		// Find a data server with lower load as migration target
		targetDS := hb.findLowLoadDataServer(dsSet)
		if targetDS == nil {
			continue
		}

		// Check if hot shard exists
		_, err := manager.GetShardManager().GetShard(hotShard.ID)
		if err != nil {
			continue
		}

		// Create hot shard split task
		taskID := "balance-hot-shard-split-" + time.Now().Format("20060102150405") + "-" + string(rand.Intn(1000))
		task := scheduler.NewBaseTask(taskID, scheduler.PriorityHigh, fmt.Sprintf("balance-hot-shard-split-%d", hotShard.ID))

		// Generate new shard ID
		newShardID := hb.generateNewShardID(allShardIDs)

		// Add shard split operation
		splitOp := operation.NewSplitShardOperation(dsWithHotShard.Addr(), uint64(hotShard.ID), uint64(newShardID), 10)
		task.AddOperation(splitOp)

		// Submit task to global scheduler
		if err := scheduler.GetTaskScheduler().SubmitTask(task); err != nil {
			log.Errorf("Failed to submit hot shard split task for shard %v: %v", hotShard.ID, err)
			continue
		}

		log.Infof("Submitted hot shard split task for shard %v on dataserver %v, new shard ID: %v",
			hotShard.ID, dsWithHotShard.Addr(), newShardID)
	}
}

// findDataServerWithShard finds the data server containing the specified shard
func (hb *HotShardBalancer) findDataServerWithShard(shardID model.ShardID, dsSet map[string]*model.DataServer) *model.DataServer {
	for _, ds := range dsSet {
		shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(ds.Addr())
		if err != nil {
			continue
		}

		for _, id := range shardIDs {
			if id == shardID {
				return ds
			}
		}
	}

	return nil
}

// findLowLoadDataServer finds a data server with lower load
func (hb *HotShardBalancer) findLowLoadDataServer(dsSet map[string]*model.DataServer) *model.DataServer {
	var lowLoadDS *model.DataServer
	var minQps int64 = -1

	for _, ds := range dsSet {
		if ds.Status != model.LiveStatusActive {
			continue
		}

		if minQps == -1 || ds.Qps < minQps {
			minQps = ds.Qps
			lowLoadDS = ds
		}
	}

	return lowLoadDS
}

// generateNewShardID generates a new shard ID
func (hb *HotShardBalancer) generateNewShardID(existingShards []model.ShardID) model.ShardID {
	// Simple implementation here, more complex logic should be used in production to generate unique shard IDs
	// Consider using timestamp and random number combination
	newID := model.ShardID(time.Now().UnixNano() + int64(rand.Intn(1000)))

	// Ensure new ID does not conflict with existing IDs
	for _, id := range existingShards {
		if id == newID {
			newID++
		}
	}

	return newID
}
