package server

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

type Rebalancer struct {
	stop chan struct{}
}

func NewRebalancer() *Rebalancer {
	return &Rebalancer{}
}

var (
	rebalancer     *Rebalancer
	rebalancerOnce sync.Once
)

func GetRebalance() *Rebalancer {
	rebalancerOnce.Do(func() {
		rebalancer = NewRebalancer()
	})

	return rebalancer
}

func (rb *Rebalancer) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)

	out:
		for {
			select {
			case <-ticker.C:
				rb.doRebalceByCapacity()

			case <-rb.stop:
				break out
			}
		}

		log.Info("rebalancer stopped ...")
	}()

	return nil
}

func (rb *Rebalancer) Stop() {
	close(rb.stop)
}

func (rb *Rebalancer) doRebalceByCapacity() {

}
