package kvengine

import (
	"errors"
	"net/url"
	"sync"
	"time"

	client "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/utils/log"
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
		etcdNode = NewEtcdNode(config.GetConfiguration())
	})
	return etcdNode
}

func GetEtcdClient() *client.Client {
	if GetEtcdNode() == nil || GetEtcdNode().client == nil {
		panic("etcd is not init!")
	}
	return GetEtcdNode().client
}

func NewEtcdNode(config *config.Configuration) *EtcdNode {
	cfg := embed.NewConfig()
	cfg.Dir = config.EtcdDataDir
	cfg.WalDir = config.EtcdWalDir

	cfg.Name = config.EtcdName
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	cfg.LCUrls = []url.URL{*config.EtcdClientAddr}
	cfg.LPUrls = []url.URL{*config.EtcdPeerAddr}
	// cfg.LogOutput = config.LogFile
	cfg.LogLevel = "info"
	cfg.InitialCluster = config.EtcdClusterPeers

	cfg.ACUrls = cfg.LCUrls
	cfg.APUrls = cfg.LPUrls

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
		log.Error("failed to start embed etcd node, err: %v", err)
		return err
	}

	select {
	case <-e.Server.ReadyNotify():
		log.Info("embed etcd is ready")
	case <-time.After(time.Minute):
		e.Server.Stop()
		log.Error("failed to start embed etcd")
		return errors.New("start etcd timeout")
	}

	en.etcdServer = e

	en.client, err = client.New(client.Config{
		Endpoints:   []string{config.MsConfig.EtcdClientAddr.String()},
		DialTimeout: config.MsConfig.EtcdClientTimeout,
		TLS:         nil,
	})

	if err != nil {
		log.Error("failed to create clientv3, err: %v", err)
		return err
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
		return errors.New("stop etcd timeout")
	}

	return nil
}

func MustStartEmbedEtcd() {
	en := GetEtcdNode()
	if err := en.Start(); err != nil {
		panic("failed to start embed etcd server")
	}

	log.Info("start embed etcd ...")
}
