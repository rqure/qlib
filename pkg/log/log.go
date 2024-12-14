package log

import (
	"fmt"
	"path"
	"runtime"
	"time"

	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Level int

const (
	UNSPECIFIED Level = iota
	TRACE
	DEBUG
	INFO
	WARN
	ERROR
	PANIC
)

var currentLogLevel int = int(TRACE)

func (l Level) String() string {
	switch l {
	case TRACE:
		return "TRACE"
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case PANIC:
		return "PANIC"
	default:
		return "UNKNOWN"
	}
}

func SetLevel(level Level) {
	currentLogLevel = int(level)
}

func GetLevel() Level {
	return Level(currentLogLevel)
}

func getCallerInfo(skip int) string {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return fmt.Sprintf("%s:%d", path.Base(file), line)
	}

	return fmt.Sprintf("%s:%d %s", path.Base(file), line, path.Base(fn.Name()))
}

func Log(level protobufs.LogMessage_LogLevelEnum, message string, args ...interface{}) {
	if int(level) < currentLogLevel {
		return
	}

	logMsg := &protobufs.LogMessage{
		Level:       level,
		Message:     fmt.Sprintf(message, args...),
		Timestamp:   timestamppb.Now(),
		Application: getCallerInfo(3),
	}

	// skip runtime.Caller + Log + helper function (Trace/Debug/etc)
	fmt.Printf("%s | %s | %s | %s\n",
		logMsg.Timestamp.AsTime().Local().Format(time.RFC3339Nano),
		logMsg.Level.String(),
		logMsg.Application,
		Truncate(logMsg.Message, 1024))
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
