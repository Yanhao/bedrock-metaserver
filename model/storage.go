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
	ID           StorageID `json:"id"`
	Name         string    `json:"name"`
	IsDeleted    bool      `json:"is_deleted"`
	DeleteTs     time.Time `json:"delete_ts"`
	RecycleTs    time.Time `json:"recycle_ts"`
	CreateTs     time.Time `json:"create_ts"`
	Owner        string    `json:"owner"`
	LastShardISN ShardISN  `json:"last_shard_isn"`
}

func (s *Storage) Copy() *Storage {
	var ret Storage
	if err := copier.Copy(&ret, s); err != nil {
		log.Errorf("err: %v stack: %s", err, string(debug.Stack()))
		panic(fmt.Sprintf("copy storage struct failed, err: %v", err))
	}

	return &ret
}

// Custom MarshalJSON method to convert time.Time fields to Unix timestamps for serialization
func (s *Storage) MarshalJSON() ([]byte, error) {
	type Alias Storage

	return json.Marshal(&struct {
		DeleteTs  int64 `json:"delete_ts"`
		RecycleTs int64 `json:"recycle_ts"`
		CreateTs  int64 `json:"create_ts"`
		*Alias
	}{
		DeleteTs:  s.DeleteTs.Unix(),
		RecycleTs: s.RecycleTs.Unix(),
		CreateTs:  s.CreateTs.Unix(),
		Alias:     (*Alias)(s),
	})
}

// Custom UnmarshalJSON method to convert Unix timestamps to time.Time fields for deserialization
func (s *Storage) UnmarshalJSON(data []byte) error {
	type Alias Storage

	aux := &struct {
		DeleteTs  int64 `json:"delete_ts"`
		RecycleTs int64 `json:"recycle_ts"`
		CreateTs  int64 `json:"create_ts"`
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
