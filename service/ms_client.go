package service

import (
	"sync"

	cache "github.com/hashicorp/golang-lru/v2"
	grpc "google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
)

type MetaServerApi struct {
	addr     string
	client   MetaServiceClient
	grpcConn *grpc.ClientConn
}

const MaxConnections int = 10

type Connections struct {
	connCaches *cache.Cache[string, MetaServerApi]
}

func NewConnections(cap int) *Connections {
	c, err := cache.NewWithEvict(cap, func(key string, cli MetaServerApi) {
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
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
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
		return cli.client, nil
	}

	newApi, err := NewMetaServerApi(addr)
	if err != nil {
		log.Error("failed to create metaserver client, addr: %s, err: %v", addr, err)
		return nil, err
	}

	cns.connCaches.Add(addr, *newApi)

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
