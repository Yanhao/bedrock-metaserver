package dataserver

import (
	"errors"
	"fmt"
	"sync"

	cache "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
)

const MaxConnections int = 10000

type Connections struct {
	connCaches *cache.Cache[string, DsApi]
}

func NewConnections(cap int) *Connections {
	c, err := cache.NewWithEvict(cap, func(key string, cli DsApi) {
		cli.Close()
	})
	if err != nil {
		panic(fmt.Sprintf("init dataserver client failed, err: %v", err))
	}

	return &Connections{
		connCaches: c,
	}
}

func (cns *Connections) GetApiClient(addr string) (DsApi, error) {
	if addr == "" {
		return nil, errors.New("empty address")
	}

	cli, ok := cns.connCaches.Get(addr)
	if ok {
		return cli.(*DataServerApi), nil
	}

	newCli, err := NewDataServerApi(addr)
	if err != nil {
		log.Error("failed to create dataserver client, addr: %s, err: %v", addr, err)
		return nil, err
	}

	cns.connCaches.Add(addr, newCli)

	return newCli, nil
}

var (
	dataServerConns     *Connections
	dataServerConnsOnce sync.Once
)

func GetDataServerConns() *Connections {
	dataServerConnsOnce.Do(func() {
		dataServerConns = NewConnections(MaxConnections)
	})

	return dataServerConns
}
