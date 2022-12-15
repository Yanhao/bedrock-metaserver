package model

import (
	"net"
	"time"

	"github.com/jinzhu/copier"
)

const (
	LiveStatusActive = iota
	LiveStatusInactive
	LiveStatusOffline
)

const DATASERVER_OVERLOAD_PERCENT = 0.9

type DataServer struct {
	Ip              string
	Port            string
	Capacity        uint64
	Free            uint64
	LastHeartBeatTs time.Time
	CreateTs        time.Time
	DeleteTs        time.Time
	Status          LiveStatus
	LastSyncTs      uint64
}

func (d *DataServer) Addr() string {
	return net.JoinHostPort(d.Ip, d.Port)
}

func (d *DataServer) Copy() *DataServer {
	var ret DataServer
	copier.Copy(&ret, d)

	return &ret
}

func (d *DataServer) IsOverLoaded() bool {
	return d.UsedPercent() > DATASERVER_OVERLOAD_PERCENT
}

func (d *DataServer) UsedPercent() float64 {
	return float64(d.Used()) / float64(d.Capacity)
}

func (d *DataServer) Used() uint64 {
	return d.Capacity - d.Free
}