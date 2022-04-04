package server

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

type GarbageCleaner struct {
	stop chan struct{}
}

func NewGarbageCleaner() *GarbageCleaner {
	return &GarbageCleaner{}
}

var (
	garbageCleaner     *GarbageCleaner
	garbageCleanerOnce sync.Once
)

func GetGarbageCleaner() *GarbageCleaner {
	garbageCleanerOnce.Do(func() {
		garbageCleaner = NewGarbageCleaner()
	})

	return garbageCleaner
}

func (gc *GarbageCleaner) Start() error {
	ticker := time.NewTicker(time.Second * 30)

out:
	for {
		select {
		case <-ticker.C:
			gc.doGarbageClean()
		case <-gc.stop:
			break out
		}
	}

	log.Info("garbage cleaner stopped ...")

	return nil
}

func (gc *GarbageCleaner) Stop() {
	close(gc.stop)
}

func (gc *GarbageCleaner) doGarbageClean() {

}
