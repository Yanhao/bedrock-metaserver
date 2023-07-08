package bg_task

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/model"
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

type HeartBeater struct {
	stop chan struct{}
}

func NewHeartBeater() *HeartBeater {
	return &HeartBeater{
		stop: make(chan struct{}),
	}
}

var (
	heartBeater     *HeartBeater
	heartBeaterOnce sync.Once
)

func GetHeartBeater() *HeartBeater {
	heartBeaterOnce.Do(func() {
		heartBeater = NewHeartBeater()
		heartBeater.InitDataServers()
	})
	return heartBeater
}

func (hb *HeartBeater) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)

	out:
		for {
			select {
			case <-ticker.C:
				hb.doHandleHeartBeat()
			case <-hb.stop:
				break out
			}
		}

		log.Info("heartbeater stopped")
	}()

	return nil
}

func (hb *HeartBeater) Stop() {
	close(hb.stop)
	hb.Reset()
}

func (hb *HeartBeater) Reset() {
	hb.stop = make(chan struct{})
}

func (hb *HeartBeater) InitDataServers() {
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

func (hb *HeartBeater) doHandleHeartBeat() {
	log.Info("handle heartbeat ...")

	if !config.GetConfiguration().EnableHeartBeatChecker {
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

	err := scheduler.ClearDataserver(server.Addr())
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
