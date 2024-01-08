package main

import (
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
)

var log = logrus.New()

// init logrus
func initLogger(logDir string) {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0644)
	}

	log.SetReportCaller(true)

	log.SetLevel(logrus.InfoLevel)
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors:   true,
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	infoLog, err := rotatelogs.New(logDir+"/%Y%m%d.info.log",
		rotatelogs.WithLinkName("info.log"),
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(24*time.Hour),
	)
	if err != nil {
		fmt.Println("init rotate log failed.")
		panic(err)
	}
	//errorLog, err := rotatelogs.New(logDir+"/%Y%m%d.error.log",
	//	rotatelogs.WithLinkName("error.log"),
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
