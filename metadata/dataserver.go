package metadata

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	client "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata/pbdata"
)

const (
	LiveStatusActive = iota
	LiveStatusInactive
	LiveStatusOffline
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

func DataServerAdd(dataServer *DataServer) error {
	_, ok := DataServers[dataServer.Addr()]
	if ok {
		return fmt.Errorf("%s already in the cluster", dataServer.Addr())
	}

	DataServers[dataServer.Addr()] = dataServer

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
