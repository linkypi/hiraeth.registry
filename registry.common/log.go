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

var Log = logrus.New()

type Formatter struct {
}

const (
	red    = 31
	yellow = 33
	blue   = 36
	gray   = 37
)

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

	Log.SetReportCaller(true)
	Log.SetLevel(logLevel)
	Log.SetFormatter(&Formatter{})

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

	Log.SetOutput(io.MultiWriter(os.Stdout, infoLog))
	//log.SetOutput(io.MultiWriter(os.Stderr, errorLog))
	Log.Info("log init success")
}
