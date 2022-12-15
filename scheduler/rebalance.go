package scheduler

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

type Rebalancer struct {
	stop chan struct{}
}

func NewRebalancer() *Rebalancer {
	return &Rebalancer{
		stop: make(chan struct{}),
	}
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
				rb.doRebalanceByCapacity()

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
	rb.Reset()
}

func (rb *Rebalancer) Reset() {
	rb.stop = make(chan struct{})
}

func (rb *Rebalancer) doRebalanceByCapacity() {

}
