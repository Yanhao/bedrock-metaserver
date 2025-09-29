package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/balancer"
	"sr.ht/moyanhao/bedrock-metaserver/health_checker"
)

func RunAsFollower() {
	log.Info(color.GreenString("execute runAsFollower."))

	balancer.GetDsCapacityBalancer().Stop()
	log.Info("stop rebalance ...")

	health_checker.GetHealthChecker().Stop()
	log.Info("stop health checker ...")
}
