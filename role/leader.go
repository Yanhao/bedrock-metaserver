package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/balancer"
	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
)

func RunAsLeader() {
	log.Info(color.GreenString("execute runAsLeader."))

	dm := manager.GetDataServerManager()
	dm.ClearCache()
	log.Info("clear dataserver cache ...")

	err := dm.LoadAllDataServers()
	if err != nil {
		log.Errorf("failed to load dataservers from etcd, err: %v", err)
	}
	log.Info("load dataservers from kv ...")

	err = health_checker.GetHealthChecker().Start()
	if err != nil {
		log.Errorf("failed to start heartbeater, err: %v", err)
	}
	log.Info("start health checker ...")

	err = balancer.GetDsCapacityBalancer().Start()
	if err != nil {
		log.Errorf("failed to start rebalance, err: %v", err)
	}
	log.Info("start rebalancer ...")

	manager.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")

	manager.GetStorageManager().ClearCache()
	log.Info("clear storage cache ...")

	err = manager.GetStorageManager().LoadLastStorageId()
	if err != nil {
		log.Errorf("failed to load last storage id from kv, err: %v", err)
	}
}
