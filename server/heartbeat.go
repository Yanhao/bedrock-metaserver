package server

import (
	"sync"
	"time"

	"github.com/jinzhu/copier"

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
	metadata.DataServersLock.Lock()
	defer metadata.DataServersLock.Unlock()

	for _, s := range metadata.DataServers {
		if s.LastHeartBeatTs.Before(time.Now().Add(-Offlineperiod)) {
			s.MarkOffline()

			OfflineDataServers[s.Addr()] = s
			delete(ActiveDataServers, s.Addr())
			delete(InactiveDataServers, s.Addr())

			var ds *metadata.DataServer
			err := copier.Copy(ds, s)
			if err != nil {
				log.Error("copy metadata.DataServer failed, err: %v", err)
			}
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

		s.MarkActive(true)
		ActiveDataServers[s.Addr()] = s
		delete(InactiveDataServers, s.Addr())
		delete(OfflineDataServers, s.Addr())
	}
}

func repairDataInServer(server *metadata.DataServer) {
	log.Info("start repair data in dataserver: %s", server.Addr())

	err := scheduler.ClearDataserver(server.Addr())
	if err != nil {
		log.Error("failed to clear data in dataserver %v, err: %v", server.Addr(), err)

		return
	}

	metadata.DataServerRemove(server.Addr())
	delete(OfflineDataServers, server.Addr())

	log.Info("successfully repair data in dataserver: %s", server.Addr())
}
