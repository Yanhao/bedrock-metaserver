package role

import (
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"sr.ht/moyanhao/bedrock-metaserver/bg_task"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
)

func RunAsFollower() {
	log.Info(color.GreenString("execute runAsFollower."))

	bg_task.GetGarbageCleaner().Stop()
	log.Info("stop garbage cleaner ...")

	scheduler.GetChecker().Stop()
	log.Info("stop checker ...")

	scheduler.GetRebalance().Stop()
	log.Info("stop rebalace ...")

	bg_task.GetHeartBeater().Stop()
	log.Info("stop heartbeater ...")
}
