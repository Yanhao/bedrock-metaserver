package manager

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"sr.ht/moyanhao/bedrock-metaserver/dal/dao"
	"sr.ht/moyanhao/bedrock-metaserver/dal/dto"
	"sr.ht/moyanhao/bedrock-metaserver/kvengine"
	"sr.ht/moyanhao/bedrock-metaserver/model"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
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

func (dm *DataServerManager) LoadDataServersFromEtcd() error {
	ec := kvengine.GetEtcdClient()
	resp, err := ec.Get(context.Background(), dao.KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return err
	}

	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	for _, kv := range resp.Kvs {
		pbDataServer := &dto.DataServer{}

		err := proto.Unmarshal(kv.Value, pbDataServer)
		if err != nil {
			log.Warn("failed to decode dataserver from pb")
			return err
		}

		dataserver := &model.DataServer{
			Ip:   pbDataServer.Ip,
			Port: pbDataServer.Port,
		}
		log.Info("load dataserver: %v", dataserver.Addr())

		addr := dataserver.Addr()
		dm.dataServers[addr] = dataserver
	}

	log.Info("load dataservers: %#v", dm.dataServers)

	return nil
}

func (dm *DataServerManager) AddDataServer(ip, port string) error {
	addr := net.JoinHostPort(ip, port)
	if dm.IsDataServerExists(addr) {
		log.Warn("dataserver %v already in the cluster", addr)
		return fmt.Errorf("%s already in the cluster", addr)
	}

	dataserver := &model.DataServer{
		Ip:              ip,
		Port:            port,
		LastHeartBeatTs: time.Now(),
		CreateTs:        time.Now(),
		Status:          model.LiveStatusActive,
	}

	err := dao.KvPutDataServer(dataserver)
	if err != nil {
		log.Error("failed put dataserver %v to kv", dataserver.Addr())
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

	return dao.KvDeleteDataServer(addr)
}

func (dm *DataServerManager) GetDataServer(addr string) (*model.DataServer, error) {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	server, ok := dm.dataServers[addr]
	if !ok {
		return nil, ErrNoSuchDataServer
	}

	var ds model.DataServer
	copier.Copy(&ds, server)

	return &ds, nil
}

func (dm *DataServerManager) IsDataServerExists(addr string) bool {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	_, ok := dm.dataServers[addr]
	return ok
}

func (dm *DataServerManager) DataServersCopy() map[string]*model.DataServer {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	log.Info("dataservers: %#v", dm.dataServers)
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

	var ds model.DataServer
	copier.Copy(&ds, d)

	go dao.KvPutDataServer(&ds)

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

	var ds model.DataServer
	copier.Copy(&ds, d)
	go dao.KvPutDataServer(&ds)

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

	var ds model.DataServer
	copier.Copy(&ds, d)
	go dao.KvPutDataServer(&ds)
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

	var ds model.DataServer
	copier.Copy(&ds, d)
	go dao.KvPutDataServer(&ds)
	return nil
}
