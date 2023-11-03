package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/bg_task"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

func RunAsLeader() {
	log.Info(color.GreenString("execute runAsLeader."))

	dm := manager.GetDataServerManager()
	dm.ClearCache()
	log.Info("clear dataserver cache ...")

	err := dm.LoadDataServersFromKv()
	if err != nil {
		log.Errorf("failed to load dataservers from etcd, err: %v", err)
	}
	log.Info("load dataservers from kv ...")

	err = bg_task.GetHeartBeater().Start()
	if err != nil {
		log.Errorf("failed to start heartbeater, err: %v", err)
	}
	log.Info("start heartbeater ...")

	err = scheduler.GetRebalance().Start()
	if err != nil {
		log.Errorf("failed to start rebalance, err: %v", err)
	}
	log.Info("start rebalance ...")

	err = bg_task.GetGarbageCleaner().Start()
	if err != nil {
		log.Errorf("failed to start garbage cleaner, err: %v", err)
	}
	log.Info("start garbage cleaner ...")

	manager.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")

	manager.GetStorageManager().ClearCache()
	log.Info("clear storage cache ...")

	err = manager.GetStorageManager().LoadLastStorageId()
	if err != nil {
		log.Errorf("failed to load last storage id from kv, err: %v", err)
	}
}
