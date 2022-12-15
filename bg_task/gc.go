package bg_task

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

type GarbageCleaner struct {
	stop chan struct{}
}

func NewGarbageCleaner() *GarbageCleaner {
	return &GarbageCleaner{
		stop: make(chan struct{}),
	}
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
	go func() {
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
	}()

	return nil
}

func (gc *GarbageCleaner) Stop() {
	close(gc.stop)
	gc.Reset()
}

func (gc *GarbageCleaner) Reset() {
	gc.stop = make(chan struct{})
}

func (gc *GarbageCleaner) doGarbageClean() {

}
