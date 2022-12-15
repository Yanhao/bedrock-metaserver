package log

import (
	"log"
	"os"

	"go.uber.org/zap"
)

const (
	LOG_DEBUG = iota
	LOG_INFO
	LOG_WARN
	LOG_ERROR
	LOG_FATAL
)

var (
	logLevel    int = LOG_INFO
	logFile     *os.File
	logger      *log.Logger
	sugarLogger *zap.SugaredLogger
)

func MustInitLogFile(file string) {
	if file == "" {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
		logFile = os.Stdout

		return
	}

	logfile, err := os.Open(file)
	if err != nil {
		panic("failed to open log file")
	}
	logFile = logfile
	logger = log.New(logFile, "", log.LstdFlags)

	return
}

func InitLogZap() error {
	l, err := zap.NewProduction()
	if err != nil {
		return err
	}

	sugarLogger = l.Sugar()
	return nil
}

func Fini() {
	if logFile != nil {
		_ = logFile.Close()
	}

	if sugarLogger != nil {
		sugarLogger.Sync()
	}
}

func SetLogLevel(level int) {
	logLevel = level
}

func Debug(a ...interface{}) {
	if logFile != nil {
		if logLevel > LOG_DEBUG {
			return
		}
		logger.Printf("DEBUG: "+a[0].(string)+"\n", a[1:]...)

		return
	}

	sugarLogger.Debug(a)
}

func Info(a ...interface{}) {
	if logFile != nil {
		if logLevel > LOG_INFO {
			return
		}
		logger.Printf("INFO: "+a[0].(string)+"\n", a[1:]...)

		return
	}

	sugarLogger.Info(a)
}

func Warn(a ...interface{}) {
	if logFile != nil {
		if logLevel > LOG_WARN {
			return
		}
		logger.Printf("WARN: "+a[0].(string)+"\n", a[1:]...)

		return
	}

	sugarLogger.Warn(a)
}

func Error(a ...interface{}) {
	if logFile != nil {
		logger.Printf("ERROR: "+a[0].(string)+"\n", a[1:]...)

		return
	}

	sugarLogger.Error(a)
}

func Fatal(a ...interface{}) {
	if logFile != nil {
		logger.Printf("FATAL: "+a[0].(string)+"\n", a[1:]...)

		return
	}

	sugarLogger.Fatal(a)
}
