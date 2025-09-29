package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"sr.ht/moyanhao/bedrock-metaserver/clients/metaserver"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
	"sr.ht/moyanhao/bedrock-metaserver/role"
	"sr.ht/moyanhao/bedrock-metaserver/scheduler"
	"sr.ht/moyanhao/bedrock-metaserver/utils"
)

func startGrpcServer() {
	lis, err := net.Listen("tcp", config.GetConfig().Server.Addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %v\n", config.GetConfig().Server.Addr))
	}

	opts := []grpc.ServerOption{}

	grpcServer := grpc.NewServer(opts...)
	metaserver.RegisterMetaServiceServer(grpcServer, &MetaService{})

	log.Infof("start grpc server at %s", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		panic("failed to start grpc server")
	}
}

type CustomFormatter struct{}

func (f *CustomFormatter) Format(entry *log.Entry) ([]byte, error) {
	var (
		file string
		line int
	)

	if entry.HasCaller() {
		file = entry.Caller.File
		line = entry.Caller.Line
	}

	_, filename := filepath.Split(file)
	log := fmt.Sprintf("%s %s %s:%d - %s\n",
		entry.Time.Format(time.RFC3339), strings.ToUpper(entry.Level.String()), filename, line, entry.Message)

	return []byte(log), nil
}

func mustInitLog() {
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&CustomFormatter{})
}

func main() {
	configFile := flag.String("config", "", "specify the configuration file")
	help := flag.Bool("help", false, "display this help information")

	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(-1)
	}

	mustInitLog()

	log.Info("metaserver starting ...")

	config.MustLoadConfig(*configFile)
	log.Info("loadding configruation ...")

	utils.SetupStackTrap()
	log.Info("setup stack trap routine ...")

	utils.SetupHttpPprof()
	log.Info("setup http pprof ...")

	log.Info("init logging ...")
	meta_store.MustStartEmbedEtcd()

	role.MustInitLeaderShip(meta_store.GetEtcdClient(), role.RunAsLeader, role.RunAsFollower)

	// Set leader check function for scheduler to determine if tasks can be submitted
	scheduler.SetLeaderChecker(func() bool {
		leadership := role.GetLeaderShip()
		return leadership != nil && leadership.IsMetaServerLeader()
	})

	startGrpcServer()

	log.Info("metaserver stop here")
}
