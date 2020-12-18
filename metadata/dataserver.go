package metadata

import "fmt"

type DataServer struct {
	Ip   uint32
	Port uint16

	Capacity uint64
	Free     uint64
}

func (d *DataServer) Used() uint64 {
	return d.Capacity - d.Free
}

func (d *DataServer) UsedPercent() float64 {
	return float64(d.Used()) / float64(d.Capacity)
}

func (d *DataServer) String() string {
	return fmt.Sprintf("%v", *d)
}
