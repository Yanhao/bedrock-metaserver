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
	"sr.ht/moyanhao/bedrock-metaserver/messages"
	"sr.ht/moyanhao/bedrock-metaserver/metadata"
	"sr.ht/moyanhao/bedrock-metaserver/service"
)

func runAsFollower() {
	log.Info(color.GreenString("start runAsFollower."))

	GetHeartBeater().Stop()
	log.Info("stop heartbeater ...")

}

func runAsLeader() {
	log.Info(color.GreenString("start runAsLeader."))

	err := GetHeartBeater().Run()
	if err != nil {
		log.Error("failed to start heartbeater, err: %v", err)
	}
	log.Info("start heartbeater ...")

	metadata.GetShardManager().ClearCache()
	log.Info("clear shard cache ...")
}

func StartGrpcServer() {
	lis, err := net.Listen("tcp", config.MsConfig.ServerAddr)
	if err != nil {
		log.Info("failed to listen on %v\n", config.MsConfig.ServerAddr)
		os.Exit(-1)
	}

	opts := []grpc.ServerOption{}

	grpcServer := grpc.NewServer(opts...)
	messages.RegisterMetaServiceServer(grpcServer, &service.MetaService{})

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

	if err := log.Init(*logFile); err != nil {
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
