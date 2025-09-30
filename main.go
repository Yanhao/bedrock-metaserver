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
	"sr.ht/moyanhao/bedrock-metaserver/interceptor"
	"sr.ht/moyanhao/bedrock-metaserver/meta_store"
	"sr.ht/moyanhao/bedrock-metaserver/role"
	"sr.ht/moyanhao/bedrock-metaserver/utils"
)

func startGrpcServer() {
	lis, err := net.Listen("tcp", config.GetConfig().Server.Addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on %v\n", config.GetConfig().Server.Addr))
	}

	// Add error interceptors to gRPC server options
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(interceptor.UnaryErrorInterceptor()),
		grpc.StreamInterceptor(interceptor.StreamErrorInterceptor()),
	}

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

// mustInitLog initializes the logger with the specified log level
// Returns error if initialization fails instead of panicking
func mustInitLog(level string) error {
	// log.SetFormatter(&log.JSONFormatter{})
	logLevel, err := log.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level: %s, err: %w", level, err)
	}
	log.SetLevel(logLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&CustomFormatter{})
	return nil
}

func main() {
	configFile := flag.String("config", "", "specify the configuration file")
	help := flag.Bool("help", false, "display this help information")

	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(-1)
	}

	log.Info("metaserver starting ...")

	config.MustLoadConfig(*configFile)
	log.Info("loading configuration ...")

	if err := mustInitLog(config.GetConfig().Server.LogLevel); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	log.Info("init logging ...")

	utils.SetupStackTrap()
	log.Info("setup stack trap routine ...")

	utils.SetupHttpPprof()
	log.Info("setup http pprof ...")

	meta_store.MustStartEmbedEtcd()

	etcdClient, err := meta_store.GetEtcdClient()
	if err != nil {
		panic(fmt.Sprintf("failed to get etcd client: %v", err))
	}
	role.MustInitLeaderShip(etcdClient, role.RunAsLeader, role.RunAsFollower)

	startGrpcServer()

	log.Info("metaserver stop here")
}
