package metadata

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/dataserver"
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
	status          LiveStatus
}

var DataServers map[string]*DataServer

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
	d.status = LiveStatusActive
	if isHeartBeat {
		d.LastHeartBeatTs = time.Now()
	}
}

func (d *DataServer) MarkInactive() {
	d.status = LiveStatusInactive
}

func (d *DataServer) MarkOffline() {
	d.status = LiveStatusOffline
}

func (d *DataServer) Addr() string {
	ipStr := strconv.FormatUint(uint64(d.Ip), 32)
	portStr := strconv.FormatUint(uint64(d.Port), 32)

	return net.JoinHostPort(ipStr, portStr)
}

// func DataServerAdd(dataServer *DataServer) error {
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

	return nil
}

func DataServerRemove(addr string) error {
	delete(DataServers, addr)

	return nil
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

func DataServerSave(dataServer *DataServer) error {
	pbDataServer := &pbdata.DataServer{
		Ip:   dataServer.Ip,
		Port: dataServer.Port,
	}

	value, err := proto.Marshal(pbDataServer)
	if err != nil {
		log.Warn("failed to encode dataserver to pb, dataserver=%v", dataServer)
		return err
	}

	addr := net.JoinHostPort(
		strconv.FormatInt(int64(pbDataServer.Ip), 10),
		strconv.FormatInt(int64(pbDataServer.Port), 10))

	ec := kv.GetEtcdClient()
	_, err = ec.Put(context.Background(), DataServerKey(addr), string(value))
	if err != nil {
		log.Warn("failed to save dataserver to etcd")
		return err
	}

	return nil
}

func DataServerRemoveFromEtcd(addr string) error {
	ec := kv.GetEtcdClient()
	_, err := ec.Delete(context.Background(), DataServerKey(addr))
	if err != nil {
		log.Warn("failed to delete dataserver from etcd")
		return err
	}

	return nil
}

func TransferShard(shardID ShardID, fromAddr, toAddr string) error {
	sm := GetShardManager()
	shard, err := sm.GetShard(shardID)
	if err != nil {
		return err
	}

	if _, ok := shard.Replicates[fromAddr]; !ok {
		return errors.New("shard not found in dataserver")
	}

	if _, ok := shard.Replicates[toAddr]; ok {
		return errors.New("shard already in dataserver")
	}

	fromCli := dataserver.GetDataServerConns().GetApiClient(toAddr)
	err = fromCli.TransferShard(uint64(shardID), toAddr)
	if err != nil {
		return err
	}

	shard.RemoveReplicates([]string{fromAddr})
	shard.AddReplicates([]string{toAddr})

	return sm.PutShard(shard)
}

func GetDataServerAddrs() []string {
	var addrs []string

	for addr := range DataServers {
		addrs = append(addrs, addr)
	}

	return addrs
}

// func RemoveDataServer(addr string) error {
// 	_, ok := DataServers[addr]
// 	if !ok {
// 		return ErrNoSuchDataServer
// 	}

// 	shardIDs, err := GetShardsInDataServer(addr)
// 	if err != nil {
// 		return errors.New("")
// 	}

// 	sm := GetShardManager()
// 	for _, shardID := range shardIDs {
// 		shard, err := sm.GetShard(shardID)
// 		if err != nil {
// 			return err
// 		}

// 		shard.RemoveReplicates(addrs []string)

// 	}

// 	return nil
// }

func IsDataServerActive(addr string) bool {
	_, ok := DataServers[addr]
	return ok
}
