package log

import (
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var applicationName string
var currentLogLevel int

func SetApplicationName(name string) {
	applicationName = name
}

func SetLogLevel(level int) {
	currentLogLevel = level
}

func Log(level protobufs.LogMessage_LogLevelEnum, message string, args ...interface{}) {
	if int(level) < currentLogLevel {
		return
	}

	logMsg := &protobufs.LogMessage{
		Level:       level,
		Message:     fmt.Sprintf(message, args...),
		Timestamp:   timestamppb.Now(),
		Application: applicationName,
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
