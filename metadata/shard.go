package metadata

type Replicate interface {
}

type ShardID uint64

type Shard struct {
	ID         ShardID
	Replicates []*Replicate
}

func (sd *Shard) Info() {
}

func (sd *Shard) Transfer() {
}

func (sd *Shard) Create() {
}

func (sd *Shard) Delete() {
}

func (sd *Shard) MarkDelete() {
}

func (sd *Shard) MarkUndelete() {
}

func (sd *Shard) Repair() {
}
