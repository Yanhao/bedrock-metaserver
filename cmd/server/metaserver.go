package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"

	"time"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/bg_task"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/manager"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/service"
	"sr.ht/moyanhao/bedrock-metaserver/utils"
)

func runAsFollower() {
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

func runAsLeader() {
	log.Info(color.GreenString("execute runAsLeader."))

	dm := manager.GetDataServerManager()
	dm.ClearCache()
	log.Info("clear dataserver cache ...")

	err := dm.LoadDataServersFromKv()
	if err != nil {
		log.Error("failed to load dataservers from etcd, err: %v", err)
	}
	log.Info("load dataservers from kv ...")

	err = bg_task.GetHeartBeater().Start()
	if err != nil {
		log.Error("failed to start heartbeater, err: %v", err)
	}
	log.Info("start heartbeater ...")

	err = scheduler.GetRebalance().Start()
	if err != nil {
		log.Error("failed to start rebalance, err: %v", err)
	}
	log.Info("start rebalance ...")

	err = scheduler.GetChecker().Start()
	if err != nil {
		log.Error("failed to start checker, err: %v", err)
	}
	log.Info("start checker ...")

	err = bg_task.GetGarbageCleaner().Start()
	if err != nil {
		log.Error("failed to start garbage cleaner, err: %v")
	}
	log.Info("start garbage cleaner ...")

	manager.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")

	manager.GetStorageManager().ClearCache()
	log.Info("clear storage cache ...")

	err = manager.GetStorageManager().LoadLastStorageId()
	if err != nil {
		log.Error("failed to load last storage id from kv, err: %v", err)
	}
}

func StartGrpcServer() {
	lis, err := net.Listen("tcp", config.MsConfig.ServerAddr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %v\n", config.MsConfig.ServerAddr))
	}

	opts := []grpc.ServerOption{}

	grpcServer := grpc.NewServer(opts...)
	service.RegisterMetaServiceServer(grpcServer, &service.MetaService{})

	log.Info("start grpc server at %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		panic("failed to start grpc server")
	}
}

func main() {
	configFile := flag.String("config", "", "specify the configuration file")
	help := flag.Bool("help", false, "display this help information")

	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(-1)
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println("metaserver starting ...")

	config.MustLoadConfig(*configFile)
	fmt.Println("loadding configruation ...")

	utils.SetupStackTrap()
	fmt.Println("setup stack trap routine ...")

	utils.SetupHttpPprof()
	fmt.Println("setup http pprof ...")

	mustInitLog()
	fmt.Println("init logging ...")

	kv_engine.MustStartEmbedEtcd()

	kv_engine.MustInitLeaderShip(kv_engine.GetEtcdClient(), runAsLeader, runAsFollower)

	StartGrpcServer()

	log.Info("metaserver stop here")
}

func mustInitLog() {
	log.SetLevel(log.InfoLevel)
}
