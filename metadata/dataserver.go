package metadata

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

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	// "sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata/pbdata"
	"sr.ht/moyanhao/bedrock-metaserver/utils"
)

const (
	LiveStatusActive = iota
	LiveStatusInactive
	LiveStatusOffline
)

const DATASERVER_OVERLOAD_PERCENT = 0.9

var (
	ErrNoSuchDataServer = errors.New("no such dataserver")
)

type LiveStatus int

type DataServer struct {
	Ip   string
	Port string

	Capacity uint64
	Free     uint64

	LastHeartBeatTs time.Time
	CreateTs        time.Time
	DeleteTs        time.Time

	Status LiveStatus

	lock sync.RWMutex `copier:"-"`
}

func (d *DataServer) Copy() *DataServer {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var ret DataServer
	copier.Copy(&ret, d)

	return &ret
}

func (d *DataServer) Addr() string {
	return net.JoinHostPort(d.Ip, d.Port)
}

func (d *DataServer) Used() uint64 {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.Capacity - d.Free
}

func (d *DataServer) UsedPercent() float64 {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return float64(d.Used()) / float64(d.Capacity)
}

func (d *DataServer) IsOverLoaded() bool {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.UsedPercent() > DATASERVER_OVERLOAD_PERCENT
}

func (d *DataServer) String() string {
	d.lock.RLock()
	defer d.lock.RUnlock()

	statusStr := "-"
	if d.Status == LiveStatusActive {
		statusStr = "Active"
	} else if d.Status == LiveStatusInactive {
		statusStr = "Inactive"
	} else if d.Status == LiveStatusOffline {
		statusStr = "Offline"
	}

	return fmt.Sprintf(
		"DataServer{ Address: %s, Capacity: %s, Free: %v, LastHeartBeatTs: %v, CreateTs: %v, DeleteTs: %v, Status: %s }",
		d.Addr(),
		utils.SizeGB(d.Capacity),
		utils.SizeGB(d.Free),
		d.LastHeartBeatTs,
		d.CreateTs,
		d.DeleteTs,
		statusStr,
	)
}

func (d *DataServer) HeartBeat() error {
	return d.MarkActive(true)
}

func (d *DataServer) MarkActive(isHeartBeat bool) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.Status = LiveStatusActive
	if isHeartBeat {
		d.LastHeartBeatTs = time.Now()
	}

	var ds DataServer
	copier.Copy(&ds, d)
	go kvPutDataServer(&ds)

	return nil
}

func (d *DataServer) MarkInactive() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.Status = LiveStatusInactive

	var ds DataServer
	copier.Copy(&ds, d)
	go kvPutDataServer(&ds)

	return nil
}

func (d *DataServer) MarkOffline() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.Status = LiveStatusOffline

	var ds DataServer
	copier.Copy(&ds, d)
	go kvPutDataServer(&ds)

	return nil
}

// func TransferShard(shardID ShardID, fromAddr, toAddr string) error {
// 	sm := GetShardManager()
// 	shard, err := sm.GetShard(shardID)
// 	if err != nil {
// 		return err
// 	}

// 	if _, ok := shard.Replicates[fromAddr]; !ok {
// 		return errors.New("shard not found in dataserver")
// 	}

// 	if _, ok := shard.Replicates[toAddr]; ok {
// 		return errors.New("shard already in dataserver")
// 	}

// 	fromCli := dataserver.GetDataServerConns().GetApiClient(toAddr)
// 	err = fromCli.TransferShard(uint64(shardID), toAddr)
// 	if err != nil {
// 		return err
// 	}

// 	shard.RemoveReplicates([]string{fromAddr})
// 	shard.AddReplicates([]string{toAddr})

// 	return sm.PutShard(shard)
// }

type DataServerManager struct {
	dataServers     map[string]*DataServer
	dataServersLock *sync.RWMutex
}

func NewDataServerManager() *DataServerManager {
	return &DataServerManager{
		dataServers: make(map[string]*DataServer),
	}
}

var (
	dataServerManager     *DataServerManager
	dataServerManagerOnce *sync.Once
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

	dm.dataServers = make(map[string]*DataServer)
}

func (dm *DataServerManager) LoadDataServersFromEtcd() error {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return err
	}

	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	for _, kv := range resp.Kvs {
		pbDataServer := &pbdata.DataServer{}

		err := proto.Unmarshal(kv.Value, pbDataServer)
		if err != nil {
			log.Warn("failed to decode dataserver from pb")
			return err
		}

		dataserver := &DataServer{
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

	dataserver := &DataServer{
		Ip:              ip,
		Port:            port,
		LastHeartBeatTs: time.Now(),
		CreateTs:        time.Now(),
		Status:          LiveStatusActive,
	}

	err := kvPutDataServer(dataserver)
	if err != nil {
		log.Error("failed put dataserver %v to kv", dataserver.Addr())
		return err
	}

	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	dm.dataServers[addr] = dataserver

	return nil
}

func (dm *DataServerManager) RemoveDataServer(addr string) error {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	delete(dm.dataServers, addr)

	return kvDeleteDataServer(addr)
}

func (dm *DataServerManager) GetDataServer(addr string) (*DataServer, error) {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	server, ok := dm.dataServers[addr]
	if !ok {
		return nil, ErrNoSuchDataServer
	}

	return server, nil
}

func (dm *DataServerManager) IsDataServerExists(addr string) bool {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	_, ok := dm.dataServers[addr]
	return ok
}

func (dm *DataServerManager) DataServersClone() map[string]*DataServer {
	dm.dataServersLock.RLock()
	defer dm.dataServersLock.RUnlock()

	log.Info("dataservers: %#v", dm.dataServers)
	ret := make(map[string]*DataServer)

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

func (dm *DataServerManager) ClearDataserverCache() {
	dm.dataServersLock.Lock()
	defer dm.dataServersLock.Unlock()

	dm.dataServers = make(map[string]*DataServer)
}
