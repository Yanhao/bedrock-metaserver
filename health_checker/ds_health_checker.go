package health_checker

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
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
	return // FIXME: remove this line

	err := ClearDataserver(server.Addr())
	if err != nil {
		log.Errorf("failed to clear data in dataserver %v, err: %v", server.Addr(), err)

		return
	}

	dm := manager.GetDataServerManager()
	dm.RemoveDataServer(server.Addr())

	// FIXME: protect by lock
	delete(OfflineDataServers, server.Addr())

	log.Infof("successfully repair data in dataserver: %s", server.Addr())
}
