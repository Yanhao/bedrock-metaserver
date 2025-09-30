package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/balancer"
	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

func RunAsFollower() {
	log.Info(color.GreenString("execute runAsFollower."))

	// Stop all tasks before becoming follower
	scheduler.GetTaskScheduler().StopAllTasks()
	log.Info("stopped all tasks as follower")

	// Stop all balancers
	balancer.GetDsCapacityBalancer().Stop()
	log.Info("stop ds capacity balancer ...")

	balancer.GetDsCpuBalancer().Stop()
	log.Info("stop ds cpu balancer ...")

	balancer.GetHotShardBalancer().Stop()
	log.Info("stop hot shard balancer ...")

	balancer.GetShardLeaderBalancer().Stop()
	log.Info("stop shard leader balancer ...")

	balancer.GetShardSizeBalancer().Stop()
	log.Info("stop shard size balancer ...")

	health_checker.GetHealthChecker().Stop()
	log.Info("stop health checker ...")
}
