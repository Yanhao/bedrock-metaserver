package metadata

type StorageID uint64

type Storage struct {
}

func NewStorage() *Storage {
	return &Storage{}
}

var StorageList map[StorageID]*Storage

func StorageCreate() {
}

func StorageDelete() {
}

func StorageInfo() {
}

func StorageRealDelete() {
}

func StorageUndelete() {
}

func StorageRename() {
}
