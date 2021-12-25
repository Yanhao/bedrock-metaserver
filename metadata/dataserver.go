package metadata

import "fmt"

type DataServer struct {
	Ip   uint32
	Port uint32

	Capacity uint64
	Free     uint64

	Shards []*Shard
}

var DataServers []DataServer

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
