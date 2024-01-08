package main

import (
	"flag"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core"
)

func main() {

	configPath := flag.String("config", "./config/registry1.conf", "The path of the configuration file,"+
		" reads the registry1.conf file in the config folder of the current location by default")
	flag.Parse()
	log.Info("config file path: %s", configPath)

	serverConfig := config.ParseConfig(*configPath, log)
	initLogger(serverConfig.LogDir)

	node := core.NewNode(serverConfig, log)
	node.Start()
}
