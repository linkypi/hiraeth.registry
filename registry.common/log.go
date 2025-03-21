package common

import (
	"bytes"
	"fmt"
	hclog "github.com/hashicorp/go-hclog"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"
)

var logx = logrus.New()

type Formatter struct {
}

const (
	red    = 31
	yellow = 33
	blue   = 36
	gray   = 37
)

// 包级别日志函数
var (
	Trace  = logx.Trace
	Tracef = logx.Tracef
	Debug  = logx.Debug
	Debugf = logx.Debugf
	Info   = logx.Info
	Infof  = logx.Infof
	Warn   = logx.Warn
	Warnf  = logx.Warnf
	Error  = logx.Error
	Errorf = logx.Errorf
	Fatal  = logx.Fatal
	Fatalf = logx.Fatalf
	Panic  = logx.Panic
	Panicf = logx.Panicf
)

func GetLogger() *logrus.Logger {
	return logx
}

// WithFields 暴露WithFields方法
func WithFields(fields logrus.Fields) *logrus.Entry {
	return logx.WithFields(fields)
}
func (t Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	// display colors according to different levels
	var levelColor int
	switch entry.Level {
	case logrus.DebugLevel, logrus.TraceLevel:
		levelColor = gray
	case logrus.WarnLevel:
		levelColor = yellow
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = red
	default:
		levelColor = blue
	}

	var buffer *bytes.Buffer
	if entry.Buffer != nil {
		buffer = entry.Buffer
	} else {
		buffer = &bytes.Buffer{}
	}

	timestamp := entry.Time.Format("2006-01-02 15:04:06.000")
	id := GetGoroutineId()
	var customFields string
	if len(entry.Data) > 0 {
		for key, value := range entry.Data {
			customFields += fmt.Sprintf(" %s=%v", key, value)
		}
	}

	if entry.HasCaller() {
		filePath := getActualCaller(entry)
		// customize the output format
		fmt.Fprintf(buffer, "[%s] \033[%dm[%s]\033[0m %s [%d] %s%s \n", timestamp,
			levelColor, entry.Level, filePath, id, entry.Message, customFields)

	} else {
		fmt.Fprintf(buffer, "[%s] \033[%dm[%s]\033[0m %s%s \n",
			timestamp, levelColor, entry.Level, entry.Message, customFields)
	}
	return buffer.Bytes(), nil
}
func getActualCaller(entry *logrus.Entry) string {
	// 在调用栈中查找第一个非common包的调用者
	for skip := 5; skip < 10; skip++ {
		_, file, line, ok := runtime.Caller(skip)
		if ok && !strings.Contains(file, "registry.common") && !strings.Contains(file, "logrus") {
			return fmt.Sprintf("%s:%d", path.Base(file), line)
		}
	}
	if !strings.Contains(entry.Caller.File, "logrus") {
		return fmt.Sprintf("%s:%d", path.Base(entry.Caller.File), entry.Caller.Line)
	}
	// 最终保底返回空值
	return "unknown:0"
}

// InitLogger init logrus
func InitLogger(logDir string, logLevel logrus.Level) {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		_ = os.MkdirAll(logDir, 0755)
	}

	logx.SetReportCaller(true)
	logx.SetLevel(logLevel)
	logx.SetFormatter(&Formatter{})

	// 配置日志分割
	infoWriter := createRotateWriter(logDir, "info")
	errorWriter := createRotateWriter(logDir, "error")

	// 设置输出
	mw := io.MultiWriter(
		os.Stdout,
		infoWriter,
	)
	logx.SetOutput(mw)

	// 添加错误级别hook
	logx.AddHook(&ErrorHook{Writer: errorWriter})

	logx.Info("log init success")
}

// 创建分割日志writer
func createRotateWriter(logDir string, level string) io.Writer {
	writer, err := rotatelogs.New(
		path.Join(logDir, fmt.Sprintf("%s.%%Y%%m%%d.log", level)),
		rotatelogs.WithLinkName(path.Join(logDir, level+".log")),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create %s log writer: %v", level, err))
	}
	return writer
}

// ErrorHook 错误级别hook
type ErrorHook struct {
	Writer io.Writer
}

func (hook *ErrorHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
	}
}

func (hook *ErrorHook) Fire(entry *logrus.Entry) error {
	line, err := entry.Logger.Formatter.Format(entry)
	if err != nil {
		return err
	}
	_, err = hook.Writer.Write(line)
	return err
}

type LoggerWrapper struct {
	*logrus.Logger
	_name string
}

func (l *LoggerWrapper) Log(level hclog.Level, msg string, args ...interface{}) {
	entry := l.WithFields(toFields(args...))
	switch level {
	case hclog.Trace:
		entry.Trace(msg)
	case hclog.Debug:
		entry.Debug(msg)
	case hclog.Info:
		entry.Info(msg)
	case hclog.Warn:
		entry.Warn(msg)
	case hclog.Error:
		entry.Error(msg)
	}
}

func (l *LoggerWrapper) Trace(msg string, args ...interface{}) {
	l.WithFields(toFields(args...)).Trace(msg)
}

func (l *LoggerWrapper) Debug(msg string, args ...interface{}) {
	l.WithFields(toFields(args...)).Debug(msg)
}

func (l *LoggerWrapper) Info(msg string, args ...interface{}) {
	l.WithFields(toFields(args...)).Info(msg)
}

func (l *LoggerWrapper) Warn(msg string, args ...interface{}) {
	l.WithFields(toFields(args...)).Warn(msg)
}

func (l *LoggerWrapper) Error(msg string, args ...interface{}) {
	l.WithFields(toFields(args...)).Error(msg)
}

func (l *LoggerWrapper) IsTrace() bool {
	return l.Level >= logrus.TraceLevel
}

func (l *LoggerWrapper) IsDebug() bool {
	return l.Level >= logrus.DebugLevel
}

func (l *LoggerWrapper) IsInfo() bool {
	return l.Level >= logrus.InfoLevel
}

func (l *LoggerWrapper) IsWarn() bool {
	return l.Level >= logrus.WarnLevel
}

func (l *LoggerWrapper) IsError() bool {
	return l.Level >= logrus.ErrorLevel
}

func (l *LoggerWrapper) ImpliedArgs() []interface{} {
	return []interface{}{}
}

func (l *LoggerWrapper) With(args ...interface{}) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{})
}

func (l *LoggerWrapper) Name() string {
	return l._name
}

func (l *LoggerWrapper) SetName(name string) {
	l._name = name
}

func (l *LoggerWrapper) Named(name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Name: name})
}

func (l *LoggerWrapper) ResetNamed(name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Name: name})
}

func (l *LoggerWrapper) SetLevel(level hclog.Level) {
	l.Logger.SetLevel(logrus.Level(level))
}

func (l *LoggerWrapper) GetLevel() hclog.Level {
	return hclog.Level(l.Logger.Level)
}

func (l *LoggerWrapper) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	if opts == nil {
		opts = &hclog.StandardLoggerOptions{}
	}
	writer := &logrusWriter{
		logger: l.Logger,
		level:  logrus.InfoLevel, // 默认使用 Info 级别
	}
	return log.New(writer, l._name, 0)
}

func (l *LoggerWrapper) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return l.Out
}

func toFields(args ...interface{}) logrus.Fields {
	fields := make(logrus.Fields)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			fields[fmt.Sprint(args[i])] = args[i+1]
		} else {
			fields[fmt.Sprint(args[i])] = ""
		}
	}
	return fields
}

type logrusWriter struct {
	logger *logrus.Logger
	level  logrus.Level
}

func (w *logrusWriter) Write(p []byte) (n int, err error) {
	msg := string(p)
	switch w.level {
	case logrus.TraceLevel:
		w.logger.Trace(msg)
	case logrus.DebugLevel:
		w.logger.Debug(msg)
	case logrus.InfoLevel:
		w.logger.Info(msg)
	case logrus.WarnLevel:
		w.logger.Warn(msg)
	case logrus.ErrorLevel:
		w.logger.Error(msg)
	case logrus.FatalLevel:
		w.logger.Fatal(msg)
	case logrus.PanicLevel:
		w.logger.Panic(msg)
	}
	return len(p), nil
}
