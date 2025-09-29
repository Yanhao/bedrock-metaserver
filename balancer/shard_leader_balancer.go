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

// ShardLeaderBalancer balances the number of leader shards across data servers
const (
	// LeaderBalanceInterval is the interval for checking leader balance
	LeaderBalanceInterval = time.Minute * 5
	// LeaderBalanceThreshold is the threshold for leader count difference to trigger balancing
	LeaderBalanceThreshold = 2
)

// dsLeaderCount stores data server and leader count information

type dsLeaderCount struct {
	ds        *model.DataServer
	leaderNum int
}

type ShardLeaderBalancer struct {
	stop chan struct{}
}

func NewShardLeaderBalancer() *ShardLeaderBalancer {
	return &ShardLeaderBalancer{
		stop: make(chan struct{}),
	}
}

var (
	shardLeaderBalancer     *ShardLeaderBalancer
	shardLeaderBalancerOnce sync.Once
)

func GetShardLeaderBalancer() *ShardLeaderBalancer {
	shardLeaderBalancerOnce.Do(func() {
		shardLeaderBalancer = NewShardLeaderBalancer()
	})

	return shardLeaderBalancer
}

func (slb *ShardLeaderBalancer) Start() error {
	go func() {
		ticker := time.NewTicker(LeaderBalanceInterval)
		defer ticker.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				slb.doRebalanceLeaders()

			case <-slb.stop:
				break out
			}
		}

		log.Info("shard leader balancer stopped ...")
	}()

	return nil
}

func (slb *ShardLeaderBalancer) Stop() {
	close(slb.stop)
	slb.stop = make(chan struct{})
}

func (slb *ShardLeaderBalancer) doRebalanceLeaders() {
	// Get all data servers
	dsSet := manager.GetDataServerManager().GetDataServersCopy()

	var dsLeaderCounts []dsLeaderCount
	var totalLeaderCount int

	for _, ds := range dsSet {
		if ds.Status != model.LiveStatusActive {
			continue
		}

		// Get shard IDs on the data server
		shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(ds.Addr())
		if err != nil {
			continue
		}

		// Count the number of leader shards
		leaderNum := 0
		for _, shardID := range shardIDs {
			shard, err := manager.GetShardManager().GetShard(shardID)
			if err != nil {
				continue
			}

			if shard.Leader == ds.Addr() {
				leaderNum++
				totalLeaderCount++
			}
		}

		dsLeaderCounts = append(dsLeaderCounts, dsLeaderCount{
			ds:        ds,
			leaderNum: leaderNum,
		})
	}

	// No balancing needed if there are no active data servers or leader shards
	if len(dsLeaderCounts) == 0 || totalLeaderCount == 0 {
		return
	}

	// Calculate the ideal number of leader shards per data server
	expectedLeaderPerDS := totalLeaderCount / len(dsLeaderCounts)

	// Identify data servers with too many or too few leader shards
	var leaderRichDS []dsLeaderCount
	var leaderPoorDS []dsLeaderCount

	for _, dsCount := range dsLeaderCounts {
		if dsCount.leaderNum > expectedLeaderPerDS+LeaderBalanceThreshold {
			leaderRichDS = append(leaderRichDS, dsCount)
		} else if dsCount.leaderNum < expectedLeaderPerDS-LeaderBalanceThreshold {
			leaderPoorDS = append(leaderPoorDS, dsCount)
		}
	}

	// No balancing needed
	if len(leaderRichDS) == 0 || len(leaderPoorDS) == 0 {
		return
	}

	// Perform leader migrations
	slb.migrateLeaders(leaderRichDS, leaderPoorDS, expectedLeaderPerDS)
}

func (slb *ShardLeaderBalancer) migrateLeaders(leaderRichDS []dsLeaderCount, leaderPoorDS []dsLeaderCount, expectedLeaderPerDS int) { // dsLeaderCount is defined in doRebalanceLeaders
	for i := 0; i < len(leaderRichDS) && i < len(leaderPoorDS); i++ {
		richDS := leaderRichDS[i]
		poorDS := leaderPoorDS[i]

		// Calculate the number of leader shards to migrate
		diffRich := richDS.leaderNum - expectedLeaderPerDS
		diffPoor := expectedLeaderPerDS - poorDS.leaderNum
		migrateCount := diffRich
		if diffPoor < migrateCount {
			migrateCount = diffPoor
		}

		// Get leader shards on the rich data server
		var richDSLeaderShards []*model.Shard
		shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(richDS.ds.Addr())
		if err != nil {
			continue
		}

		for _, shardID := range shardIDs {
			shard, err := manager.GetShardManager().GetShard(shardID)
			if err != nil {
				continue
			}

			if shard.Leader == richDS.ds.Addr() {
				richDSLeaderShards = append(richDSLeaderShards, shard)
			}
		}

		// Randomly select some leader shards to migrate
		if len(richDSLeaderShards) > 0 {
			// Shuffle shard list randomly
			rand.Shuffle(len(richDSLeaderShards), func(i, j int) {
				richDSLeaderShards[i], richDSLeaderShards[j] = richDSLeaderShards[j], richDSLeaderShards[i]
			})

			// Migrate as many shards as possible
			actualMigrateCount := 0
			for _, shard := range richDSLeaderShards {
				// Check if target data server is a replica of this shard
				isReplica := false
				for replica := range shard.Replicates {
					if replica == poorDS.ds.Addr() {
						isReplica = true
						break
					}
				}

				if !isReplica {
					continue
				}

				// Create leader transfer task
				slb.createAndSubmitLeaderTransferTask(shard.ID(), richDS.ds.Addr(), poorDS.ds.Addr())

				actualMigrateCount++
				if actualMigrateCount >= migrateCount {
					break
				}
			}
		}
	}
}

func (slb *ShardLeaderBalancer) createAndSubmitLeaderTransferTask(shardID model.ShardID, sourceDS, targetDS string) {
	// Create task ID
	taskID := "balance-leader-transfer-" + time.Now().Format("20060102150405") + "-" + string(rand.Intn(1000))

	// Create base task
	task := scheduler.NewBaseTask(taskID, scheduler.PriorityMedium, fmt.Sprintf("balance-leader-transfer-shard-%d", shardID))

	// Create leader transfer operation
	// Use a slice to contain the target server
	targets := []string{targetDS}
	transferOp := operation.NewTransferLeaderOperation(sourceDS, uint64(shardID), targets, 5)
	task.AddOperation(transferOp)

	// Submit task to scheduler
	taskScheduler := scheduler.NewTaskScheduler(5)
	if err := taskScheduler.Start(); err != nil {
		log.Errorf("Failed to start task scheduler: %v", err)
		return
	}
	if err := taskScheduler.SubmitTask(task); err != nil {
		log.Errorf("Failed to submit leader transfer task for shard %v: %v", shardID, err)
		return
	}

	log.Infof("Submitted leader transfer task for shard %v from %v to %v",
		shardID, sourceDS, targetDS)
}
