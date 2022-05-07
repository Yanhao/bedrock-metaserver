package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	// "sr.ht/moyanhao/bedrock-metaserver/dataserver"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata/pbdata"
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
	Ip   uint32
	Port uint32

	Capacity uint64
	Free     uint64

	LastHeartBeatTs time.Time
	CreatedTs       time.Time
	DeletedTs       time.Time

	Status LiveStatus

	lock sync.RWMutex `copier:"-"`
}

var DataServers map[string]*DataServer
var DataServersLock *sync.RWMutex

func init() {
	DataServers = make(map[string]*DataServer)
	DataServersLock = &sync.RWMutex{}
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

	return fmt.Sprintf("%v", *d)
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
	go putDataServerToKv(&ds)

	return nil
}

func (d *DataServer) MarkInactive() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.Status = LiveStatusInactive

	var ds DataServer
	copier.Copy(&ds, d)
	go putDataServerToKv(&ds)

	return nil
}

func (d *DataServer) MarkOffline() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.Status = LiveStatusOffline

	var ds DataServer
	copier.Copy(&ds, d)
	go putDataServerToKv(&ds)

	return nil
}

func (d *DataServer) Addr() string {
	ipStr := strconv.FormatUint(uint64(d.Ip), 10)
	portStr := strconv.FormatUint(uint64(d.Port), 10)

	return net.JoinHostPort(ipStr, portStr)
}

func DataServerAdd(ip, port string) error {
	addr := net.JoinHostPort(ip, port)
	if !IsDataServerExists(addr) {
		return fmt.Errorf("%s already in the cluster", addr)
	}

	ipInt, _ := strconv.ParseUint(ip, 10, 32)
	portInt, _ := strconv.ParseUint(port, 10, 32)

	dataserver := &DataServer{
		Ip:   uint32(ipInt),
		Port: uint32(portInt),
	}

	err := putDataServerToKv(dataserver)
	if err != nil {
		return err
	}

	DataServersLock.Lock()
	defer DataServersLock.Unlock()

	DataServers[addr] = dataserver
	return nil
}

func DataServerRemove(addr string) error {
	DataServersLock.Lock()
	defer DataServersLock.Unlock()

	delete(DataServers, addr)

	return deleteDataServerFromKv(addr)
}

func DataServersClone() map[string]*DataServer {
	DataServersLock.RLock()
	defer DataServersLock.RUnlock()

	ret := make(map[string]*DataServer)

	copier.CopyWithOption(&ret, DataServers, copier.Option{IgnoreEmpty: true, DeepCopy: true})

	return ret
}

func LoadDataServersFromEtcd() error {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return err
	}

	DataServersLock.Lock()
	defer DataServersLock.Unlock()

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

		addr := net.JoinHostPort(
			strconv.FormatInt(int64(pbDataServer.Ip), 10),
			strconv.FormatInt(int64(pbDataServer.Port), 10))

		DataServers[addr] = dataserver
	}

	return nil
}

func GetDataServerAddrs() []string {
	DataServersLock.RLock()
	defer DataServersLock.RUnlock()

	var addrs []string
	for addr := range DataServers {
		addrs = append(addrs, addr)
	}

	return addrs
}

func IsDataServerExists(addr string) bool {
	DataServersLock.RLock()
	defer DataServersLock.RUnlock()

	_, ok := DataServers[addr]
	return ok
}

func GetDataServerByAddr(addr string) (*DataServer, error) {
	DataServersLock.RLock()
	defer DataServersLock.RUnlock()

	server, ok := DataServers[addr]
	if !ok {
		return nil, ErrNoSuchDataServer
	}

	return server, nil
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
