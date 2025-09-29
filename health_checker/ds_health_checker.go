package health_checker

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/operation"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

// make sure the following data no need to be locked
var (
	ActiveDataServers   map[string]*model.DataServer
	InactiveDataServers map[string]*model.DataServer
	OfflineDataServers  map[string]*model.DataServer
)

const (
	InactivePeriod = time.Second * 30
	OfflinePeriod  = time.Minute * 30
)

type HealthChecker struct {
	stop chan struct{}
}

func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		stop: make(chan struct{}),
	}
}

var (
	healthChecker     *HealthChecker
	healthCheckerOnce sync.Once
)

func GetHealthChecker() *HealthChecker {
	healthCheckerOnce.Do(func() {
		healthChecker = NewHealthChecker()
		healthChecker.InitDataServers()
	})
	return healthChecker
}

func (hc *HealthChecker) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)

	out:
		for {
			select {
			case <-ticker.C:
				hc.doHealthCheck()
			case <-hc.stop:
				break out
			}
		}

		log.Info("heartbeater stopped")
	}()

	return nil
}

func (hc *HealthChecker) Stop() {
	close(hc.stop)
	hc.stop = make(chan struct{})
}

func (hc *HealthChecker) InitDataServers() {
	log.Info("init dataservers ...")

	ActiveDataServers = make(map[string]*model.DataServer)
	InactiveDataServers = make(map[string]*model.DataServer)
	OfflineDataServers = make(map[string]*model.DataServer)

	dm := manager.GetDataServerManager()
	dataservers := dm.GetDataServersCopy()

	for _, d := range dataservers {
		switch d.Status {
		case model.LiveStatusActive:
			ActiveDataServers[d.Addr()] = d
		case model.LiveStatusInactive:
			InactiveDataServers[d.Addr()] = d
		case model.LiveStatusOffline:
			OfflineDataServers[d.Addr()] = d
		}
	}
}

func (hc *HealthChecker) doHealthCheck() {
	log.Info("handle heartbeat ...")

	if !config.GetConfig().Server.EnableHeartBeatChecker {
		return
	}

	dm := manager.GetDataServerManager()
	dataservers := dm.GetDataServersCopy()

	for _, s := range dataservers {
		if s.LastHeartBeatTs.Before(time.Now().Add(-OfflinePeriod)) {
			dm.MarkOffline(s.Addr())

			OfflineDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(InactiveDataServers, s.Addr())

			ds := s.Copy()
			go repairDataInServer(ds)

			continue
		}

		if s.LastHeartBeatTs.Before(time.Now().Add(-InactivePeriod)) {
			dm.MarkInactive(s.Addr())

			InactiveDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(OfflineDataServers, s.Addr())

			continue
		}

		_ = dm.MarkActive(s.Addr(), false)
		ActiveDataServers[s.Addr()] = s
		delete(InactiveDataServers, s.Addr())
		delete(OfflineDataServers, s.Addr())
	}
}

func repairDataInServer(server *model.DataServer) {
	log.Infof("start repair data in dataserver: %s", server.Addr())

	// Get all shards on this data server
	shardIDs, err := manager.GetShardManager().GetShardIDsInDataServer(server.Addr())
	if err != nil {
		log.Errorf("failed to get shard IDs in dataserver %v, err: %v", server.Addr(), err)
		return
	}

	// Create a repair task for each shard
	taskScheduler := scheduler.NewTaskScheduler(5)
	if err := taskScheduler.Start(); err != nil {
		log.Errorf("Failed to start task scheduler: %v", err)
		return
	}

	for _, shardID := range shardIDs {
		// Get shard information
		shard, err := manager.GetShardManager().GetShard(shardID)
		if err != nil {
			log.Errorf("failed to get shard %v, err: %v", shardID, err)
			continue
		}

		// Create shard repair task
		taskID := "repair-shard-" + time.Now().Format("20060102150405") + "-" + string(rand.Intn(1000))
		task := scheduler.NewBaseTask(taskID, scheduler.PriorityUrgent, fmt.Sprintf("repair-shard-%d", shardID))

		// Select an active data server as target
		activeDS := selectActiveDataServer()
		if activeDS == nil {
			log.Errorf("no active dataserver available to repair shard %v", shardID)
			continue
		}

		// Create repair shard operation
		createOp := operation.NewCreateShardOperation(activeDS.Addr(), uint64(shardID), shard.RangeKeyStart, shard.RangeKeyEnd, 100)
		task.AddOperation(createOp)

		// Submit task to scheduler
		if err := taskScheduler.SubmitTask(task); err != nil {
			log.Errorf("Failed to submit repair task for shard %v: %v", shardID, err)
			continue
		}

		log.Infof("Submitted repair task for shard %v to dataserver %v", shardID, activeDS.Addr())
	}

	// Clean up and remove offline data server
	dm := manager.GetDataServerManager()
	dm.RemoveDataServer(server.Addr())

	// Remove offline data server mapping (lock protected)
	dsMutex.Lock()
	delete(OfflineDataServers, server.Addr())
	dsMutex.Unlock()

	log.Infof("successfully repair data in dataserver: %s", server.Addr())
}

// selectActiveDataServer selects an active data server
func selectActiveDataServer() *model.DataServer {
	if len(ActiveDataServers) == 0 {
		return nil
	}

	// Simple implementation: return the first active data server
	for _, ds := range ActiveDataServers {
		return ds
	}

	return nil
}

// dsMutex protects the data server maps
var dsMutex sync.RWMutex
