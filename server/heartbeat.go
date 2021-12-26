package server

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
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

}
