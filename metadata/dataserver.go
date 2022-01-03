package metadata

import (
	"fmt"
	"net"
	"strconv"
	"time"
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

func DataServerAdd() {
}

func DataServerRemove() {
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

func AddDataServer(dataServer *DataServer) error {
	_, ok := DataServers[dataServer.Addr()]
	if ok {
		return fmt.Errorf("%s already in the cluster", dataServer.Addr())
	}

	DataServers[dataServer.Addr()] = dataServer

	return nil
}
