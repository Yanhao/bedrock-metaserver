package model

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
)

type StorageID uint32

type Storage struct {
	ID           StorageID
	Name         string
	IsDeleted    bool
	DeleteTs     time.Time
	RecycleTs    time.Time
	CreateTs     time.Time
	Owner        string
	LastShardISN ShardISN
}

func (s *Storage) Copy() *Storage {
	var ret Storage
	if err := copier.Copy(&ret, s); err != nil {
		log.Error("err: %v stack:%s", err, string(debug.Stack()))
		panic(fmt.Sprintf("copy storage struct failed, err: %v", err))
	}

	return &ret
}

// 自定义 MarshalJSON 方法，将 time.Time 字段转换为 Unix 时间戳进行序列化
func (s *Storage) MarshalJSON() ([]byte, error) {
	type Alias Storage

	return json.Marshal(&struct {
		DeleteTs  int64 `json:"deleteTs"`
		RecycleTs int64 `json:"recycleTs"`
		CreateTs  int64 `json:"createTs"`
		*Alias
	}{
		DeleteTs:  s.DeleteTs.Unix(),
		RecycleTs: s.RecycleTs.Unix(),
		CreateTs:  s.CreateTs.Unix(),
		Alias:     (*Alias)(s),
	})
}

// 自定义 UnmarshalJSON 方法，将 Unix 时间戳转换为 time.Time 字段进行反序列化
func (s *Storage) UnmarshalJSON(data []byte) error {
	type Alias Storage

	aux := &struct {
		DeleteTs  int64 `json:"deleteTs"`
		RecycleTs int64 `json:"recycleTs"`
		CreateTs  int64 `json:"createTs"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.DeleteTs = time.Unix(aux.DeleteTs, 0)
	s.RecycleTs = time.Unix(aux.RecycleTs, 0)
	s.CreateTs = time.Unix(aux.CreateTs, 0)

	return nil
}
