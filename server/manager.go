package server

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/fatih/color"
	"google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/service"
)

func runAsFollower() {
	log.Info(color.GreenString("start runAsFollower."))

	GetGarbageCleaner().Stop()
	log.Info("stop garbage cleaner ...")

	scheduler.GetChecker().Stop()
	log.Info("stop checker ...")

	scheduler.GetRebalance().Stop()
	log.Info("stop rebalace ...")

	GetHeartBeater().Stop()
	log.Info("stop heartbeater ...")
}

func runAsLeader() {
	log.Info(color.GreenString("start runAsLeader."))

	err := GetHeartBeater().Start()
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

	err = GetGarbageCleaner().Start()
	if err != nil {
		log.Error("failed to start garbage cleaner, err: %v")
	}
	log.Info("start garbage cleaner ...")

	metadata.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")

	metadata.GetStorageManager().ClearCache()
	log.Info("clear storage cache ...")
}

func StartGrpcServer() {
	lis, err := net.Listen("tcp", config.MsConfig.ServerAddr)
	if err != nil {
		log.Info("failed to listen on %v\n", config.MsConfig.ServerAddr)
		os.Exit(-1)
	}

	opts := []grpc.ServerOption{}

	grpcServer := grpc.NewServer(opts...)
	service.RegisterMetaServiceServer(grpcServer, &service.MetaService{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Error("failed to start grpc server")
		os.Exit(-1)
	}
}

func Start() {
	configFile := flag.String("config", "", "specify the configuration file")
	logFile := flag.String("log", "", "specify the log file")
	help := flag.Bool("help", false, "display this help infomation")

	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(-1)
	}

	fmt.Println("metaserver starting ...")

	SetupStackTrap()
	fmt.Println("setup stack trap routine ...")

	if _, err := config.LoadConfigFromFile(*configFile); err != nil {
		fmt.Println("failed to load configuration file")
		os.Exit(-1)
	}
	config.ValidateConfig()

	SetupHttpPprof()
	fmt.Println("setup http pprof ...")

	// _ = logFile
	if err := log.InitLogFile(*logFile); err != nil {
		// if err := log.InitLogZap(); err != nil {
		fmt.Println("failed to initialize logging")
		os.Exit(-1)
	}

	defer log.Fini()

	en := kv.GetEtcdNode()
	if err := en.Start(); err != nil {
		fmt.Println("failed to start embed etcd server")
		os.Exit(-1)
	}

	stop := make(chan struct{})
	go func() {
		for {
			leaderChangeNotifier := en.LeaderShip.GetNotifier()

			select {
			case <-stop:
				log.Info("metaserver stopping")
				return
			case c := <-leaderChangeNotifier:
				switch c.Role {
				case kv.BecameFollower:
					runAsFollower()
				case kv.BecameLeader:
					runAsLeader()
				}
			}
		}
	}()

	go StartGrpcServer()

	for {
		time.Sleep(time.Second)
	}

}
