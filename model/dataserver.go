package model

import (
	"encoding/json"
	"net"
	"time"

	"github.com/jinzhu/copier"
)

const (
	LiveStatusActive = iota
	LiveStatusInactive
	LiveStatusOffline
)

const DataServerOverloadPercent = 0.9

type LiveStatus int

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
	Idc             string
}

// 自定义 MarshalJSON 方法，将 time.Time 字段转换为 Unix 时间戳进行序列化
func (ds *DataServer) MarshalJSON() ([]byte, error) {
	type Alias DataServer

	return json.Marshal(&struct {
		LastHeartBeatTs int64 `json:"lastHeartBeatTs"`
		CreateTs        int64 `json:"createTs"`
		DeleteTs        int64 `json:"deleteTs"`
		*Alias
	}{
		LastHeartBeatTs: ds.LastHeartBeatTs.Unix(),
		CreateTs:        ds.CreateTs.Unix(),
		DeleteTs:        ds.DeleteTs.Unix(),
		Alias:           (*Alias)(ds),
	})
}

func (ds *DataServer) UnmarshalJSON(data []byte) error {
	type Alias DataServer
	aux := &struct {
		LastHeartBeatTs int64 `json:"lastHeartBeatTs"`
		CreateTs        int64 `json:"createTs"`
		DeleteTs        int64 `json:"deleteTs"`
		*Alias
	}{
		Alias: (*Alias)(ds),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ds.LastHeartBeatTs = time.Unix(aux.LastHeartBeatTs, 0)
	ds.CreateTs = time.Unix(aux.CreateTs, 0)
	ds.DeleteTs = time.Unix(aux.DeleteTs, 0)

	return nil
}

func (d *DataServer) Addr() string {
	return net.JoinHostPort(d.Ip, d.Port)
}

func (d *DataServer) Copy() *DataServer {
	var ret DataServer
	if err := copier.Copy(&ret, d); err != nil {
		panic(err)
	}

	return &ret
}

func (d *DataServer) IsOverLoaded() bool {
	return d.UsedPercent() > DataServerOverloadPercent
}

func (d *DataServer) UsedPercent() float64 {
	return float64(d.Used()) / float64(d.Capacity)
}

func (d *DataServer) Used() uint64 {
	return d.Capacity - d.Free
}
