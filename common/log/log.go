package log

import (
	"errors"
	"log"
	"os"
)

const (
	LOG_DEBUG = iota
	LOG_INFO
	LOG_WARN
	LOG_ERROR
	LOG_FATAL
)

var (
	logLevel int = LOG_INFO
	logFile  *os.File
	logger   *log.Logger
)

func Init(file string) error {
	if file == "" {
		logger = log.New(os.Stdout, "", log.LstdFlags)
		return nil
	}

	logfile, err := os.Open(file)
	if err != nil {
		return errors.New("failed to open log file")
	}
	logFile = logfile

	logger = log.New(logFile, "", log.LstdFlags)
	return nil
}

func Fini() {
	_ = logFile.Close()
}

func SetLogLevel(level int) {
	logLevel = level
}

func Debug(format string, a ...interface{}) {
	if logLevel > LOG_DEBUG {
		return
	}
	logger.Println("DEBUG: "+format, a)
}

func Info(format string, a ...interface{}) {
	if logLevel > LOG_INFO {
		return
	}
	logger.Println("INFO: "+format, a)
}

func Warn(format string, a ...interface{}) {
	if logLevel > LOG_WARN {
		return
	}
	logger.Println("WARN: "+format, a)
}

func Error(format string, a ...interface{}) {
	logger.Println("ERROR: "+format, a)
}

func Fatal(format string, a ...interface{}) {
	logger.Println("FATAL: "+format, a)
}
