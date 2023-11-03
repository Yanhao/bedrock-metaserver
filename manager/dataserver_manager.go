package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"
	"sr.ht/moyanhao/bedrock-metaserver/clients/metaserver"

	"sr.ht/moyanhao/bedrock-metaserver/dal"
	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
)

var (
	ErrNoSuchDataServer = errors.New("no such dataserver")
)

type DataServerManager struct {
	dataServers     map[string]*model.DataServer
	dataServersLock sync.RWMutex
}

func NewDataServerManager() *DataServerManager {
	return &DataServerManager{
		dataServers: make(map[string]*model.DataServer),
	}
}

var (
	dataServerManager     *DataServerManager
	dataServerManagerOnce sync.Once
)

func GetDataServerManager() *DataServerManager {
	dataServerManagerOnce.Do(func() {
		dataServerManager = NewDataServerManager()
	})

	return dataServerManager
}

func (dm *DataServerManager) ClearCache() {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	dm.dataServers = make(map[string]*model.DataServer)
}

func (dm *DataServerManager) LoadDataServersFromKv() error {
	ec := kv_engine.GetEtcdClient()
	resp, err := ec.Get(context.Background(), dal.KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return err
	}

	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	for _, kv := range resp.Kvs {
		var dataserver model.DataServer

		err := dataserver.UnmarshalJSON(kv.Value)
		if err != nil {
			log.Warn("failed to decode dataserver from pb")
			return err
		}

		dm.dataServers[dataserver.Addr()] = &dataserver
	}

	log.Infof("load dataservers: %#v", dm.dataServers)

	return nil
}

func (dm *DataServerManager) AddDataServer(ip, port string) error {
	addr := net.JoinHostPort(ip, port)
	if dm.IsDataServerExists(addr) {
		log.Warnf("dataserver %v already in the cluster", addr)
		return fmt.Errorf("%s already in the cluster", addr)
	}

	dataserver := &model.DataServer{
		Ip:              ip,
		Port:            port,
		LastHeartBeatTs: time.Now(),
		CreateTs:        time.Now(),
		Status:          model.LiveStatusActive,
		FreeCapacity:    1024000, // FIXME:
		Capacity:        1024000, // FIXME:
	}

	err := dal.KvPutDataServer(dataserver)
	if err != nil {
		log.Errorf("failed put dataserver %v to kv", dataserver.Addr())
		return err
	}

	// fault injection point
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	dm.dataServers[addr] = dataserver

	return nil
}

func (dm *DataServerManager) RemoveDataServer(addr string) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	delete(dm.dataServers, addr)

	return dal.KvDeleteDataServer(addr)
}

func (dm *DataServerManager) GetDataServer(addr string) (*model.DataServer, error) {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	server, ok := dm.dataServers[addr]
	if !ok {
		return nil, ErrNoSuchDataServer
	}

	return server.Copy(), nil
}

func (dm *DataServerManager) IsDataServerExists(addr string) bool {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	_, ok := dm.dataServers[addr]
	return ok
}

func (dm *DataServerManager) GetDataServersCopy() map[string]*model.DataServer {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	log.Infof("dataservers: %#v", dm.dataServers)
	ret := make(map[string]*model.DataServer)

	copier.CopyWithOption(&ret, dm.dataServers, copier.Option{IgnoreEmpty: true, DeepCopy: true})

	return ret
}

func (dm *DataServerManager) GetDataServerAddrs() []string {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	var addrs []string
	for addr := range dm.dataServers {
		addrs = append(addrs, addr)
	}

	return addrs
}

func (dm *DataServerManager) UpdateSyncTs(addr string, syncTs int64) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	d, ok := dm.dataServers[addr]
	if !ok {
		return ErrNoSuchDataServer
	}

	d.LastSyncTs = uint64(syncTs)

	go dal.KvPutDataServer(d.Copy())

	return nil
}

func (dm *DataServerManager) MarkInactive(addr string) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	d, ok := dm.dataServers[addr]
	if !ok {
		return ErrNoSuchDataServer
	}

	d.Status = model.LiveStatusInactive

	go dal.KvPutDataServer(d.Copy())

	return nil
}

func (dm *DataServerManager) MarkActive(addr string, isHeartBeat bool) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	d, ok := dm.dataServers[addr]
	if !ok {
		return ErrNoSuchDataServer
	}

	d.Status = model.LiveStatusActive
	if isHeartBeat {
		d.LastHeartBeatTs = time.Now()
	}

	go dal.KvPutDataServer(d.Copy())

	return nil
}

func (dm *DataServerManager) MarkOffline(addr string) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	d, ok := dm.dataServers[addr]
	if !ok {
		return ErrNoSuchDataServer
	}

	d.Status = model.LiveStatusInactive

	go dal.KvPutDataServer(d.Copy())

	return nil
}

func (dm *DataServerManager) UpdateStatus(req *metaserver.HeartBeatRequest) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	d, ok := dm.dataServers[req.Addr]
	if !ok {
		return ErrNoSuchDataServer
	}

	d.FreeCapacity = req.FreeCapacity
	d.Qps = int64(req.Qps)

	clear(d.BigShards)
	clear(d.HotShards)

	for _, s := range req.BigShards {
		d.BigShards = append(d.BigShards, model.ShardIDAndSize{
			ID:   model.ShardID(s.ShardId),
			Size: int64(s.Size),
		})
	}

	for _, s := range req.HotShards {
		d.HotShards = append(d.HotShards, model.ShardIDAndQps{
			ID:  model.ShardID(s.ShardId),
			QPS: int64(s.Qps),
		})
	}

	return nil
}
