package scheduler

import (
	"math/rand"
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"sr.ht/moyanhao/bedrock-metaserver/clients/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
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

	toDsCli, err := dataserver.GetDataServerConns().GetApiClient(maxFreeCapacityDs.Addr())
	if err != nil {
		return
	}

	if err := toDsCli.CreateShard(uint64(shardID2Migrate), shard.RangeKeyStart, shard.RangeKeyEnd); err != nil {
		return
	}

	fromDsCli, err := dataserver.GetDataServerConns().GetApiClient(minFreeCapaciryDs.Addr())
	if err != nil {
		return
	}

	if err := fromDsCli.MigrateShard(uint64(shardID2Migrate), uint64(shardID2Migrate), maxFreeCapacityDs.Addr()); err != nil {
		return
	}
}
