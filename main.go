package main

import (
	"flag"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core"
)

func main() {
	configPath := flag.String("config", "./config/registry1.conf", "The path of the configuration file,"+
		" reads the registry1.conf file in the config folder of the current location by default")

	startUpMode := flag.String("startup_mode", "", "Whether to run in stand-alone mode, "+
		"[stand-alone] indicates that it runs in stand-alone mode, and [cluster] indicates that it runs in cluster mode, which is stand-alone by default")
	logLevel := flag.String("log_level", "", "specify the log level, with the following"+
		" values: debug, info, warn, error, fatal. the default log level is info")
	flag.Parse()
	log.Infof("config file path: %s", *configPath)
	conf := config.ParseConfig(*configPath, log)

	// if there are specified parameters on the command line, the parameter is used preferentially
	// and the relevant parameters in the registry.conf file are ignored
	if *startUpMode != "" {
		config.SetClusterConfigProperty(&conf.ClusterConfig, config.StartupModeProp, *startUpMode)
	}
	if *logLevel != "" {
		config.SetConfigProperty(&conf, config.LogLevelProp, *logLevel)
	}

	initLogger(conf.NodeConfig.LogDir, conf.LogLevel)

	node := core.NewNode(conf, log)
	node.Start(&conf.ClusterConfig)
}
