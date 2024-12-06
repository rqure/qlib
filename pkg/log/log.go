package log

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func Log(level protobufs.LogMessage_LogLevelEnum, message string, args ...interface{}) {
	logLevel, err := strconv.Atoi(os.Getenv("Q_LOG_LEVEL"))
	if err != nil {
		logLevel = 2
	}

	if int(level) < logLevel {
		return
	}

	logMsg := &protobufs.LogMessage{
		Level:       level,
		Message:     fmt.Sprintf(message, args...),
		Timestamp:   timestampprotobufs.Now(),
		Application: GetApplicationName(),
	}

	fmt.Printf("%s | %s | %s | %s\n", logMsg.Timestamp.AsTime().Local().Format(time.RFC3339Nano), logMsg.Application, logMsg.Level.String(), Truncate(logMsg.Message, 1024))
}

func Trace(message string, args ...interface{}) {
	Log(protobufs.LogMessage_TRACE, message, args...)
}

func Debug(message string, args ...interface{}) {
	Log(protobufs.LogMessage_DEBUG, message, args...)
}

func Info(message string, args ...interface{}) {
	Log(protobufs.LogMessage_INFO, message, args...)
}

func Warn(message string, args ...interface{}) {
	Log(protobufs.LogMessage_WARN, message, args...)
}

func Error(message string, args ...interface{}) {
	Log(protobufs.LogMessage_ERROR, message, args...)
}

func Panic(message string, args ...interface{}) {
	Log(protobufs.LogMessage_PANIC, message, args...)
	panic(message)
}
