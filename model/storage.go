package model

import (
	"time"

	"github.com/jinzhu/copier"
)

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
	copier.Copy(&ret, s)

	return &ret
}
