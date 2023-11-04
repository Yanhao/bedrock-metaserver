package scheduler

import (
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
)

type ShardSizeBalancer struct {
	stop chan struct{}
}

func NewShardSizeBalancer() *ShardSizeBalancer {
	return &ShardSizeBalancer{
		stop: make(chan struct{}),
	}
}

var (
	shardSizeBalancer     *ShardSizeBalancer
	shardSizeBalancerOnce sync.Once
)

func GetShardSizeBalancer() *ShardSizeBalancer {
	shardSizeBalancerOnce.Do(func() {
		shardSizeBalancer = NewShardSizeBalancer()
	})

	return shardSizeBalancer
}

func (s *ShardSizeBalancer) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				s.doShardSizeBalance()

			case <-s.stop:
				break out
			}
		}

		log.Info("shard size balancer stopped ...")
	}()

	return nil
}

func (s *ShardSizeBalancer) Stop() {
	close(s.stop)
	s.stop = make(chan struct{})
}

func (s *ShardSizeBalancer) doShardSizeBalance() {
	dataservers := manager.GetDataServerManager().GetDataServersCopy()

	for addr, ds := range dataservers {
		log.Infof("shard size rebalance in %s", addr)

		bigShards := slices.Clone(ds.BigShards)

		if len(bigShards) == 0 {
			continue
		}

		for _, shard := range bigShards {
			if shard.Size < config.GetConfig().Scheduler.BigShardSizeThreshold {
				continue
			}
			_ = GetShardAllocator().SplitShard(shard.ID)
		}
	}

	log.Info("finish one round of shard size rebalance")
}
