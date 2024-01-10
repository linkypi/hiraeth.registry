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

	conf := config.ParseConfig(*configPath, log)
	initLogger(conf.NodeConfig.LogDir)

	if *standAlone == "true" {
		conf.NodeConfig.StandAlone = true
	}
	node := core.NewNode(conf, log)
	node.Start()
}
