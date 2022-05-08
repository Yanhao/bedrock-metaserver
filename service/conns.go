package service

import (
	"sync"

	cache "github.com/hashicorp/golang-lru"
	grpc "google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
)

type MetaServerApi struct {
	addr     string
	client   MetaServiceClient
	grpcConn *grpc.ClientConn
}

const MaxConnections int = 10

type Connections struct {
	connCaches *cache.Cache
}

func NewConnections(cap int) *Connections {
	c, err := cache.NewWithEvict(cap, func(key, value interface{}) {
		cli, ok := value.(MetaServerApi)
		if !ok {
			return
		}

		cli.grpcConn.Close()
	})

	if err != nil {
		panic(err)
	}

	return &Connections{
		connCaches: c,
	}
}

func NewMetaServerApi(addr string) (*MetaServerApi, error) {
	conn, err := grpc.Dial(addr)
	if err != nil {
		return nil, err
	}

	c := &MetaServerApi{
		addr:     addr,
		grpcConn: conn,
		client:   NewMetaServiceClient(conn),
	}
	return c, nil
}

func (cns *Connections) GetClient(addr string) (MetaServiceClient, error) {
	cli, ok := cns.connCaches.Get(addr)
	if ok {
		return cli.(MetaServiceClient), nil
	}

	newApi, err := NewMetaServerApi(addr)
	if err != nil {
		log.Error("failed to create metaserver client, addr: %s, err: %v", addr, err)
		return nil, err
	}

	cns.connCaches.Add(addr, newApi)

	return newApi.client, nil
}

var (
	metaServerConns     *Connections
	metaServerConnsOnce sync.Once
)

func GetMetaServerConns() *Connections {
	metaServerConnsOnce.Do(func() {
		metaServerConns = NewConnections(MaxConnections)
	})

	return metaServerConns
}
