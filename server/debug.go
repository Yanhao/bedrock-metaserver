package server

import (
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

// usage:
// kill -USR1 $pid
// less stack.log

const (
	timeFormat = "2006-01-02 15:04:05"
)

var (
	stackFile = "./debug_stack.log"
)

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
