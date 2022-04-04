package server

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"sr.ht/moyanhao/bedrock-metaserver/config"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

var (
	stackFile = "./debug_stack.log"
)

// usage:
// kill -USR1 $pid
// less stack.log
func SetupStackTrap(args ...string) {
	if len(args) > 0 {
		stackFile = args[0]
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)
	go func() {
		select {
		case <-c:
			dumpStacks()
		}
	}()
}

func dumpStacks() {
	buf := make([]byte, 1<<20*10)
	buf = buf[:runtime.Stack(buf, true)]
	writeStack(buf)
}

func writeStack(buf []byte) {
	fd, _ := os.OpenFile(stackFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	defer fd.Close()

	now := time.Now().Format(timeFormat)
	fd.WriteString("\n\n\n\n\n")
	fd.WriteString(now + " stdout:" + "\n\n")
	fd.Write(buf)
}

// need to be called after configuration initialized
func SetupHttpPprof() {
	addr := config.MsConfig.PprofListenAddr

	go func() {
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			fmt.Printf("SetupHttpPprof failed, err: %v", err)
			os.Exit(-1)
		}
	}()
}
