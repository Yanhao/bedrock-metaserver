package model

import (
	"encoding/json"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
)

const (
	LiveStatusActive = iota
	LiveStatusInactive
	LiveStatusOffline
)

const DataServerOverloadPercent = 0.9

type LiveStatus int

type ShardIDAndSize struct {
	ID   ShardID `json:"id"`
	Size int64   `json:"size"`
}

type ShardIDAndQps struct {
	ID  ShardID `json:"id"`
	QPS int64   `json:"qps"`
}

type DataServer struct {
	Ip              string     `json:"ip"`
	Port            string     `json:"port"`
	Capacity        uint64     `json:"capacity"`
	FreeCapacity    uint64     `json:"free_capacity"`
	LastHeartBeatTs time.Time  `json:"last_heart_beat_ts"`
	CreateTs        time.Time  `json:"create_ts"`
	DeleteTs        time.Time  `json:"delete_ts"`
	Status          LiveStatus `json:"status"`
	LastSyncTs      uint64     `json:"last_sync_ts"`
	Idc             string     `json:"idc"`
	Qps             int64      `json:"qps"`

	BigShards []ShardIDAndSize `json:"big_shards"`
	HotShards []ShardIDAndQps  `json:"hot_shards"`
}

// 自定义 MarshalJSON 方法，将 time.Time 字段转换为 Unix 时间戳进行序列化
func (ds *DataServer) MarshalJSON() ([]byte, error) {
	type Alias DataServer

	return json.Marshal(&struct {
		LastHeartBeatTs int64 `json:"last_heart_beat_ts"`
		CreateTs        int64 `json:"create_ts"`
		DeleteTs        int64 `json:"delete_ts"`
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
		LastHeartBeatTs int64 `json:"last_heart_beat_ts"`
		CreateTs        int64 `json:"create_ts"`
		DeleteTs        int64 `json:"delete_ts"`
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
		log.Errorf("err: %v stack:%s", err, string(debug.Stack()))
		panic(fmt.Sprintf("copy dataserver struct failed, err: %v", err))
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
	return d.Capacity - d.FreeCapacity
}
