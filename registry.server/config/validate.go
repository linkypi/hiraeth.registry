package config

import (
	"github.com/linkypi/hiraeth.registry/common"
	"os"
	"regexp"
)

func validateIP(ip, key string, opts []kv) any {
	clusterRegexCompile := regexp.MustCompile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	match := clusterRegexCompile.MatchString(ip)
	if !match {
		common.Error("property " + NodeIp + " value is invalid: " + ip)
		os.Exit(1)
	}
	return ip
}

func ParseByDefaultOpts(val, key string, opts []kv) any {
	if opts == nil {
		common.Error("property " + key + "'s options must not be empty. ")
		os.Exit(1)
	}

	for _, opt := range opts {
		if val == opt.key {
			return opt.val
		}
	}

	common.Errorf("property %s's value doesn't match the elements provided in options: %s", StartupMode, val)
	os.Exit(1)
	return nil
}

func parseClusterServers(addrs, key string, opts []kv) any {
	servers, ok := common.ParseClusterServers(addrs)
	if !ok {
		common.Error("property " + ClusterServerAddr + " value is invalid: " + addrs)
		os.Exit(1)
	}
	return servers
}

func validateNumber(id, key string, opts []kv) any {
	clusterRegexCompile := regexp.MustCompile("\\d+")
	match := clusterRegexCompile.MatchString(id)
	if !match {
		common.Error("property " + NodeId + " value is invalid: " + id)
		os.Exit(1)
	}
	return id
}
