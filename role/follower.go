package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

func RunAsFollower() {
	log.Info(color.GreenString("execute runAsFollower."))

	scheduler.GetDsCapacityBalancer().Stop()
	log.Info("stop rebalance ...")

	health_checker.GetHealthChecker().Stop()
	log.Info("stop health checker ...")
}
