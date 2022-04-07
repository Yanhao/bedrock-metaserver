package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

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

var (
	ErrNoSuchDataServer = errors.New("no such dataserver")
)

type LiveStatus int

type DataServer struct {
	Ip   uint32
	Port uint32

	Capacity uint64
	Free     uint64

	Shards []*Shard

	LastHeartBeatTs time.Time
	Status          LiveStatus
}

var DataServers map[string]*DataServer
var DataServersLock *sync.RWMutex

func init() {
	DataServersLock = &sync.RWMutex{}
}

func (d *DataServer) Used() uint64 {
	return d.Capacity - d.Free
}

func (d *DataServer) UsedPercent() float64 {
	return float64(d.Used()) / float64(d.Capacity)
}

func (d *DataServer) IsOverLoaded() bool {
	return d.UsedPercent() > 0.9
}

func (d *DataServer) String() string {
	return fmt.Sprintf("%v", *d)
}

func (d *DataServer) HeartBeat() error {
	return nil
}

func (d *DataServer) MarkActive(isHeartBeat bool) {
	d.Status = LiveStatusActive
	if isHeartBeat {
		d.LastHeartBeatTs = time.Now()
	}

	putDataServerToKv(d)
}

func (d *DataServer) MarkInactive() {
	d.Status = LiveStatusInactive

	putDataServerToKv(d)
}

func (d *DataServer) MarkOffline() {
	d.Status = LiveStatusOffline

	putDataServerToKv(d)
}

func (d *DataServer) Addr() string {
	ipStr := strconv.FormatUint(uint64(d.Ip), 10)
	portStr := strconv.FormatUint(uint64(d.Port), 10)

	return net.JoinHostPort(ipStr, portStr)
}

func DataServerAdd(ip, port string) error {
	addr := net.JoinHostPort(ip, port)
	_, ok := DataServers[addr]
	if ok {
		return fmt.Errorf("%s already in the cluster", addr)

	}
	ipInt, _ := strconv.ParseUint(ip, 10, 32)
	portInt, _ := strconv.ParseUint(port, 10, 32)

	dataserver := &DataServer{
		Ip:   uint32(ipInt),
		Port: uint32(portInt),
	}

	DataServers[addr] = dataserver

	err := putDataServerToKv(dataserver)
	if err != nil {
		// TODO: remove DataServers In memory
		return err
	}

	return nil
}

func DataServerRemove(addr string) error {
	delete(DataServers, addr)

	return deleteDataServerFromKv(addr)
}

func LoadDataServersFromEtcd() error {
	ec := kv.GetEtcdClient()
	resp, err := ec.Get(context.Background(), KvPrefixDataServer, client.WithPrefix())
	if err != nil {
		log.Warn("failed to get dataserver from etcd")
		return err
	}

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

func GetDataServerAddrs() []string {
	var addrs []string

	for addr := range DataServers {
		addrs = append(addrs, addr)
	}

	return addrs
}

func IsDataServerExists(addr string) bool {
	_, ok := DataServers[addr]
	return ok
}
