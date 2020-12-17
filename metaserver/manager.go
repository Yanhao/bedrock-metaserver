package metaserver

import (
	"flag"
	"fmt"
	"os"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/config"
	"sr.ht/moyanhao/bedrock-metaserver/kv"
)

func runAsFollower() {
}

func runAsLeader() {
}

func Start() {
	fmt.Println("Starting ...")

	configFile := flag.String("config", "", "Sepcify the configuration file")
	logFile := flag.String("log", "", "Speclify the log file")
	help := flag.Bool("help", false, "Display this help infomation")

	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(-1)
	}

	if _, err := config.LoadConfigFromFile(*configFile); err != nil {
		fmt.Println("failed to load configuration file")
		os.Exit(-1)
	}

	if err := log.Init(*logFile); err != nil {
		fmt.Println("failed to initialize logging")
		os.Exit(-1)
	}

	defer log.Fini()

	en := kv.NewEtcdNode()
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

	for {
		time.Sleep(time.Second)
	}

}
