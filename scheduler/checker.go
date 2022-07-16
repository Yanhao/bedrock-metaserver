package scheduler

import (
	"sync"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

type Checker struct {
	stop chan struct{}
}

func NewChecker() *Checker {
	return &Checker{
		stop: make(chan struct{}),
	}
}

var (
	checker     *Checker
	checkerOnce sync.Once
)

func GetChecker() *Checker {
	checkerOnce.Do(func() {
		checker = NewChecker()
	})

	return checker
}

func (c *Checker) Start() error {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
	out:
		for {
			select {
			case <-ticker.C:
				c.doCheck()

			case <-c.stop:
				break out
			}
		}

		log.Info("checker stopped ...")

	}()

	return nil
}

func (c *Checker) Stop() {
	close(c.stop)
	c.Reset()
}

func (c *Checker) Reset() {
	c.stop = make(chan struct{})
}

func (c *Checker) doCheck() {

}
