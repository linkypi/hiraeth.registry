package common

import (
	"bytes"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
	"time"
)

var log = logrus.New()

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
	Trace  = log.Trace
	Tracef = log.Tracef
	Debug  = log.Debug
	Debugf = log.Debugf
	Info   = log.Info
	Infof  = log.Infof
	Warn   = log.Warn
	Warnf  = log.Warnf
	Error  = log.Error
	Errorf = log.Errorf
	Fatal  = log.Fatal
	Fatalf = log.Fatalf
	Panic  = log.Panic
	Panicf = log.Panicf
)

func GetLogger() *logrus.Logger {
	return log
}

// WithFields 暴露WithFields方法
func WithFields(fields logrus.Fields) *logrus.Entry {
	return log.WithFields(fields)
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
	if entry.HasCaller() {
		// customize the file path
		//funcName := entry.Caller.Function
		filePath := fmt.Sprintf("%s:%d", path.Base(entry.Caller.File), entry.Caller.Line)
		// customize the output format
		fmt.Fprintf(buffer, "[%s] \033[%dm[%s]\033[0m %s [%d] %s \n", timestamp,
			levelColor, entry.Level, filePath, id, entry.Message)

	} else {
		fmt.Fprintf(buffer, "[%s] \033[%dm[%s]\033[0m %s \n",
			timestamp, levelColor, entry.Level, entry.Message)
	}
	return buffer.Bytes(), nil
}

// InitLogger init logrus
func InitLogger(logDir string, logLevel logrus.Level) {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		_ = os.MkdirAll(logDir, 0755)
	}

	log.SetReportCaller(true)
	log.SetLevel(logLevel)
	log.SetFormatter(&Formatter{})

	// 配置日志分割
	infoWriter := createRotateWriter(logDir, "info")
	errorWriter := createRotateWriter(logDir, "error")

	// 设置输出
	mw := io.MultiWriter(
		os.Stdout,
		infoWriter,
	)
	log.SetOutput(mw)

	// 添加错误级别hook
	log.AddHook(&ErrorHook{Writer: errorWriter})

	infoLog, err := rotatelogs.New(logDir+"/%Y%m%d.info.log",
		rotatelogs.WithLinkName(logDir+"/info.log"),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
	if err != nil {
		fmt.Println("init rotate log failed.")
		panic(err)
	}
	//errorLog, err := rotatelogs.New(logDir+"/%Y%m%d.error.log",
	//	rotatelogs.WithLinkName(logDir+"error.log"),
	//	rotatelogs.WithMaxAge(7*24*time.Hour),
	//	rotatelogs.WithRotationTime(24*time.Hour),
	//)
	//if err != nil {
	//	fmt.Println("init rotate log failed.")
	//	panic(err)
	//}

	log.SetOutput(io.MultiWriter(os.Stdout, infoLog))
	//log.SetOutput(io.MultiWriter(os.Stderr, errorLog))
	log.Info("log init success")
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
