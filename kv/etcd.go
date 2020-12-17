package kv

import (
	"errors"
	"net/url"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/config"
)

type EtcdNode struct {
	etcdServer *embed.Etcd
	config     *embed.Config
	client     *clientv3.Client
	LeaderShip *LeaderShip
}

func NewEtcdNode() EtcdNode {
	cfg := embed.NewConfig()
	cfg.Dir = config.MsConfig.EtcdDataDir
	cfg.WalDir = config.MsConfig.EtcdWalDir

	cfg.Name = config.MsConfig.EtcdName
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	cfg.LCUrls = []url.URL{*config.MsConfig.EtcdClientAddr}
	cfg.LPUrls = []url.URL{*config.MsConfig.EtcdPeerAddr}
	cfg.LogOutput = config.MsConfig.LogFile
	cfg.InitialCluster = config.MsConfig.EtcdClusterPeers

	cfg.ACUrls = cfg.LCUrls
	cfg.APUrls = cfg.LPUrls

	cfg.QuotaBackendBytes = 0

	return EtcdNode{
		config:     cfg,
		etcdServer: nil,
		client:     nil,
		LeaderShip: nil,
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
		log.Info("embeded etcd is ready")
	case <-time.After(time.Minute):
		e.Server.Stop()
		log.Error("failed to start embed etcd")
		return errors.New("timeout")
	}

	en.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{config.MsConfig.EtcdClientAddr.String()},
		DialTimeout: config.MsConfig.EtcdClientTimeout,
		TLS:         nil,
	})

	if err != nil {
		log.Error("failed to create clientv3, err: %v", err)
		return err
	}

	en.LeaderShip, err = NewLeaderShip(e, en.client, "metaserver-leader", en.config.Name)
	go en.LeaderShip.Start()

	return nil
}

func (en *EtcdNode) Stop() error {
	en.LeaderShip.Stop()
	en.etcdServer.Close()

	return nil
}
