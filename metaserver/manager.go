package metaserver

import (
	"flag"
	"fmt"
	"os"

	"sr.ht/moyanhao/bedrock-metaserver/common/log"
	"sr.ht/moyanhao/bedrock-metaserver/config"
)

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
}
