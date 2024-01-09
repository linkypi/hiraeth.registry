package main

import (
	"flag"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core"
)

func main() {
	configPath := flag.String("config", "./config/registry1.conf", "The path of the configuration file,"+
		" reads the registry1.conf file in the config folder of the current location by default")

	standAlone := flag.String("standalone", "false", "Whether to run in stand-alone mode, "+
		"true indicates that it runs in stand-alone mode, and false indicates that it runs in cluster mode, which is false by default")
	flag.Parse()
	log.Infof("config file path: %s", *configPath)

	serverConfig := config.ParseConfig(*configPath, log)
	initLogger(serverConfig.LogDir)

	if *standAlone == "true" {
		serverConfig.StandAlone = true
	}
	node := core.NewNode(serverConfig, log)
	node.Start()
}
