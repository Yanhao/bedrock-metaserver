package meta_store

import (
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	client "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/errors"
)

type EtcdNode struct {
	etcdServer *embed.Etcd
	config     *embed.Config
	client     *client.Client
}

var (
	etcdNode     *EtcdNode
	etcdNodeOnce sync.Once
)

func GetEtcdNode() *EtcdNode {
	etcdNodeOnce.Do(func() {
		etcdNode = NewEtcdNode()
	})
	return etcdNode
}

func GetEtcdClient() (*client.Client, error) {
	en := GetEtcdNode()
	if en == nil || en.client == nil {
		return nil, errors.New(errors.ErrCodeNotInitialized, "etcd is not initialized")
	}
	return en.client, nil
}

func NewEtcdNode() *EtcdNode {
	etcdConfig := config.GetConfig().Etcd
	cfg := embed.NewConfig()
	cfg.Dir = etcdConfig.DataDir
	cfg.WalDir = etcdConfig.WalDir

	cfg.Name = etcdConfig.Name
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	clientAddrUrl, _ := url.Parse(etcdConfig.ClientAddr)
	peerAddrUrl, _ := url.Parse(etcdConfig.PeerAddr)
	log.Debugf("config: %v", config.GetConfig())
	log.Debugf("clientAddrUrl: %v, peerAddrUrl: %v", etcdConfig.ClientAddr, etcdConfig.PeerAddr)

	cfg.ListenClientUrls = []url.URL{*clientAddrUrl}
	cfg.ListenPeerUrls = []url.URL{*peerAddrUrl}
	// cfg.LogOutput = config.LogFile
	cfg.LogLevel = "warn"
	cfg.InitialCluster = etcdConfig.ClusterPeers

	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls

	cfg.QuotaBackendBytes = 0

	return &EtcdNode{
		config:     cfg,
		etcdServer: nil,
		client:     nil,
	}
}

func (en *EtcdNode) Start() error {
	e, err := embed.StartEtcd(en.config)
	if err != nil {
		log.Errorf("failed to start embed etcd node, err: %v", err)
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to start embedded etcd")
	}

	select {
	case <-e.Server.ReadyNotify():
		log.Info("embed etcd is ready")
	case <-time.After(time.Minute):
		e.Server.Stop()
		log.Error("failed to start embed etcd")
		return errors.New(errors.ErrCodeTimeout, "start etcd timeout")
	}

	en.etcdServer = e

	clientAddrUrl, _ := url.Parse(config.GetConfig().Etcd.ClientAddr)
	en.client, err = client.New(client.Config{
		Endpoints:   []string{clientAddrUrl.String()},
		DialTimeout: time.Duration(config.GetConfig().Etcd.ClientTimeoutSec) * time.Second,
		TLS:         nil,
	})

	if err != nil {
		log.Errorf("failed to create clientv3, err: %v", err)
		return errors.Wrap(err, errors.ErrCodeSystem, "failed to create etcd client")
	}

	return nil
}

func (en *EtcdNode) Stop() error {
	en.etcdServer.Server.Stop()

	select {
	case <-en.etcdServer.Server.StopNotify():
		log.Info("embed etcd is stop")
	case <-time.After(time.Minute):
		log.Error("failed to stop embed etcd")
		return errors.New(errors.ErrCodeTimeout, "stop etcd timeout")
	}

	return nil
}

func MustStartEmbedEtcd() {
	en := GetEtcdNode()
	if err := en.Start(); err != nil {
		log.Fatalf("failed to start embed etcd server, err: %v", err)
	}

	log.Info("start embed etcd ...")
}
