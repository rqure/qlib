package log

import (
	"fmt"
	"path"
	"runtime"
	"strings"
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

var currentLogLevel int = int(TRACE)    // Log level for the application
var currentLibLogLevel int = int(TRACE) // Log level for the library

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

func SetLibLevel(level Level) {
	currentLibLogLevel = int(level)
}

func GetLibLevel() Level {
	return Level(currentLibLogLevel)
}

func getCallerInfo(skip int) string {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}

	// Trim the /go/pkg/mod/github.com/ prefix from file path
	gomod := "github.com/rqure/"
	if idx := strings.Index(file, gomod); idx != -1 {
		file = file[idx+len(gomod):]
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return fmt.Sprintf("%s:%d", file, line)
	}

	return fmt.Sprintf("%s:%d | %s", file, line, path.Base(fn.Name()))
}

func getStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

func Log(level protobufs.LogMessage_LogLevelEnum, message string, args ...interface{}) {
	callInfo := getCallerInfo(3)
	calledByQlib := strings.Contains(callInfo, "qlib")

	if calledByQlib && int(level) < currentLibLogLevel {
		return
	} else if !calledByQlib && int(level) < currentLogLevel {
		return
	}

	logMsg := &protobufs.LogMessage{
		Level:       level,
		Message:     fmt.Sprintf(message, args...),
		Timestamp:   timestamppb.Now(),
		Application: callInfo,
	}

	output := fmt.Sprintf("%s | %s | %s | %s",
		logMsg.Timestamp.AsTime().Local().Format(time.RFC3339Nano),
		logMsg.Level.String(),
		logMsg.Application,
		Truncate(logMsg.Message, 1024))

	if level == protobufs.LogMessage_ERROR || level == protobufs.LogMessage_PANIC {
		output += "\nStack trace:\n" + getStackTrace()
	}

	fmt.Println(output)
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
	panic(fmt.Sprintf(message, args...))
}
