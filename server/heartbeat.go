package server

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

// make sure the following data no need to be locked
var (
	ActiveDataServers   map[string]*metadata.DataServer
	InactiveDataServers map[string]*metadata.DataServer
	OfflineDataServers  map[string]*metadata.DataServer
)

const (
	InactivePeriod = time.Second * 30
	Offlineperiod  = time.Minute * 30
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

	ActiveDataServers = make(map[string]*metadata.DataServer)
	InactiveDataServers = make(map[string]*metadata.DataServer)
	OfflineDataServers = make(map[string]*metadata.DataServer)

	metadata.DataServersLock.RLock()
	defer metadata.DataServersLock.RUnlock()

	for _, d := range metadata.DataServers {
		switch d.Status {
		case metadata.LiveStatusActive:
			ActiveDataServers[d.Addr()] = d
		case metadata.LiveStatusInactive:
			InactiveDataServers[d.Addr()] = d
		case metadata.LiveStatusOffline:
			OfflineDataServers[d.Addr()] = d
		}
	}
}

func (hb *HeartBeater) doHandleHeartBeat() {
	log.Info("handle heartbeat ...")
	return // FIXME: remove this line

	metadata.DataServersLock.Lock()
	defer metadata.DataServersLock.Unlock()

	for _, s := range metadata.DataServers {
		if s.LastHeartBeatTs.Before(time.Now().Add(-Offlineperiod)) {
			s.MarkOffline()

			OfflineDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(InactiveDataServers, s.Addr())

			ds := s.Copy()
			go repairDataInServer(ds)

			continue
		}

		if s.LastHeartBeatTs.Before(time.Now().Add(-InactivePeriod)) {
			s.MarkInactive()

			InactiveDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(OfflineDataServers, s.Addr())

			continue
		}

		_ = s.MarkActive(false)
		ActiveDataServers[s.Addr()] = s
		delete(InactiveDataServers, s.Addr())
		delete(OfflineDataServers, s.Addr())
	}
}

func repairDataInServer(server *metadata.DataServer) {
	log.Info("start repair data in dataserver: %s", server.Addr())
	return // FIXME: remove this line

	err := scheduler.ClearDataserver(server.Addr())
	if err != nil {
		log.Error("failed to clear data in dataserver %v, err: %v", server.Addr(), err)

		return
	}

	metadata.DataServerRemove(server.Addr())

	// FIXME: protect by lock
	delete(OfflineDataServers, server.Addr())

	log.Info("successfully repair data in dataserver: %s", server.Addr())
}
