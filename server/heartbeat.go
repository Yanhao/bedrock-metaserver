package server

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
)

type HeartBeater struct {
	stop chan struct{}
}

var (
	ActiveDataServers   map[string]*metadata.DataServer
	InactiveDataServers map[string]*metadata.DataServer
	OfflineDataServers  map[string]*metadata.DataServer
)

const (
	InactivePeriod = time.Second * 30
	Offlineperiod  = time.Minute * 30
)

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
	})
	return heartBeater
}

func (hb *HeartBeater) Run() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)

	out:
		for {
			select {
			case <-ticker.C:
				hb.doHeartBeat()
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
}

func (hb *HeartBeater) doHeartBeat() {
	for _, s := range metadata.DataServers {
		if s.LastHeartBeatTs.Before(time.Now().Add(-InactivePeriod)) {
			s.MarkInactive()
			InactiveDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(OfflineDataServers, s.Addr())
		}

		if s.LastHeartBeatTs.Before(time.Now().Add(-Offlineperiod)) {
			s.MarkOffline()
			OfflineDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(InactiveDataServers, s.Addr())

			go repairDataInServer(s)
		}

		s.MarkActive(true)
		ActiveDataServers[s.Addr()] = s
		delete(InactiveDataServers, s.Addr())
		delete(OfflineDataServers, s.Addr())
	}
}

func repairDataInServer(server *metadata.DataServer) {
	log.Info("start repair data in dataserver: %s", server.Addr())

	delete(OfflineDataServers, server.Addr())
	log.Info("successfully repair data in dataserver: %s", server.Addr())
}
