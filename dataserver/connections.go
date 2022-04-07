package dataserver

import (
	"sync"

	cache "github.com/hashicorp/golang-lru"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver/api"
)

const MaxConnections int = 10000

type Connections struct {
	connCaches *cache.Cache
}

func NewConnections(cap int) *Connections {
	c, err := cache.NewWithEvict(cap, func(key, value interface{}) {
		cli, ok := value.(api.DsApi)
		if !ok {
			return
		}

		cli.Close()
	})
	if err != nil {
		panic(err)
	}

	return &Connections{
		connCaches: c,
	}
}

func (cns *Connections) GetApiClient(addr string) (api.DsApi, error) {
	cli, ok := cns.connCaches.Get(addr)
	if ok {
		return cli.(*api.DataServerApi), nil
	}

	newCli, err := api.NewDataServerApi(addr)
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