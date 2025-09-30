package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/balancer"
	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
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

	// Allow task submission when becoming leader
	scheduler.GetTaskScheduler().AllowSubmission()
	log.Info("allowed task submission as leader")

	err = health_checker.GetHealthChecker().Start()
	if err != nil {
		log.Errorf("failed to start health checker, err: %v", err)
	}
	log.Info("start health checker ...")

	// Start all balancers
	err = balancer.GetDsCapacityBalancer().Start()
	if err != nil {
		log.Errorf("failed to start ds capacity balancer, err: %v", err)
	}
	log.Info("start ds capacity balancer ...")

	err = balancer.GetDsCpuBalancer().Start()
	if err != nil {
		log.Errorf("failed to start ds cpu balancer, err: %v", err)
	}
	log.Info("start ds cpu balancer ...")

	err = balancer.GetHotShardBalancer().Start()
	if err != nil {
		log.Errorf("failed to start hot shard balancer, err: %v", err)
	}
	log.Info("start hot shard balancer ...")

	err = balancer.GetShardLeaderBalancer().Start()
	if err != nil {
		log.Errorf("failed to start shard leader balancer, err: %v", err)
	}
	log.Info("start shard leader balancer ...")

	err = balancer.GetShardSizeBalancer().Start()
	if err != nil {
		log.Errorf("failed to start shard size balancer, err: %v", err)
	}
	log.Info("start shard size balancer ...")

	manager.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")

	manager.GetStorageManager().ClearCache()
	log.Info("clear storage cache ...")

	err = manager.GetStorageManager().LoadLastStorageId()
	if err != nil {
		log.Errorf("failed to load last storage id from kv, err: %v", err)
	}
}
