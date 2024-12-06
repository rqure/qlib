package qlogging

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func Log(level LogMessage_LogLevelEnum, message string, args ...interface{}) {
	logLevel, err := strconv.Atoi(os.Getenv("QDB_LOG_LEVEL"))
	if err != nil {
		logLevel = 2
	}

	if int(level) < logLevel {
		return
	}

	logMsg := &LogMessage{
		Level:       level,
		Message:     fmt.Sprintf(message, args...),
		Timestamp:   timestamppb.Now(),
		Application: GetApplicationName(),
	}

	fmt.Printf("%s | %s | %s | %s\n", logMsg.Timestamp.AsTime().Local().Format(time.RFC3339Nano), logMsg.Application, logMsg.Level.String(), Truncate(logMsg.Message, 1024))
}

func Trace(message string, args ...interface{}) {
	Log(LogMessage_TRACE, message, args...)
}

func Debug(message string, args ...interface{}) {
	Log(LogMessage_DEBUG, message, args...)
}

func Info(message string, args ...interface{}) {
	Log(LogMessage_INFO, message, args...)
}

func Warn(message string, args ...interface{}) {
	Log(LogMessage_WARN, message, args...)
}

func Error(message string, args ...interface{}) {
	Log(LogMessage_ERROR, message, args...)
}

func Panic(message string, args ...interface{}) {
	Log(LogMessage_PANIC, message, args...)
	panic(message)
}
