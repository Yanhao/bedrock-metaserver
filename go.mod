module sr.ht/moyanhao/bedrock-metaserver

go 1.15

require (
	github.com/golang/protobuf v1.5.2
	github.com/pelletier/go-toml v1.8.1
	go.etcd.io/etcd/client/v3 v3.5.1
	go.etcd.io/etcd/server/v3 v3.5.1
	golang.org/x/crypto v0.0.0-20201217014255-9d1352758620 // indirect
	google.golang.org/grpc v1.38.0
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5
