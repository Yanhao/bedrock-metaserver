package dataserver

import (
	"sync"

	"sr.ht/moyanhao/bedrock-metaserver/dataserver/api"
)

type Connections struct {
}

func NewConnections() *Connections {
	return &Connections{}
}

func (cns *Connections) GetApiClient(addr string) api.DsApi {
	panic("")
}

var (
	dataServerConns     *Connections
	dataServerConnsOnce sync.Once
)

func GetDataServerConns() *Connections {
	dataServerConnsOnce.Do(func() {
		dataServerConns = NewConnections()
	})

	return dataServerConns
}
