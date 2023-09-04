package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"

	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/clients/metaserver"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/kv_engine"
	"sr.ht/moyanhao/bedrock-metaserver/role"
	"sr.ht/moyanhao/bedrock-metaserver/utils"
)

func startGrpcServer() {
	lis, err := net.Listen("tcp", config.MsConfig.ServerAddr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %v\n", config.MsConfig.ServerAddr))
	}

	opts := []grpc.ServerOption{}

	grpcServer := grpc.NewServer(opts...)
	metaserver.RegisterMetaServiceServer(grpcServer, &MetaService{})

	log.Infof("start grpc server at %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		panic("failed to start grpc server")
	}
}

func mustInitLog() {
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)
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

	role.MustInitLeaderShip(kv_engine.GetEtcdClient(), role.RunAsLeader, role.RunAsFollower)

	startGrpcServer()

	log.Info("metaserver stop here")
}
