package config

import (
	"os"
	"regexp"
	"strings"
)

func validateIP(ip, key string, opts []kv) any {
	clusterRegexCompile := regexp.MustCompile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	match := clusterRegexCompile.MatchString(ip)
	if !match {
		log.Error("property " + NodeIp + " value is invalid: " + ip)
		os.Exit(1)
	}
	return ip
}

func ParseByDefaultOpts(val, key string, opts []kv) any {
	if opts == nil {
		log.Error("property " + key + "'s options must not be empty. ")
		os.Exit(1)
	}

	for _, opt := range opts {
		if val == opt.key {
			return opt.val
		}
	}

	log.Errorf("property %s's value doesn't match the elements provided in options: %s", StartupMode, val)
	os.Exit(1)
	return nil
}

func parseClusterServers(ctrlCandidateServers, key string, opts []kv) any {
	parts := strings.Split(ctrlCandidateServers, ",")
	clusterServers := make([]string, len(parts))

	clusterRegexCompile := regexp.MustCompile("(\\d+):(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)")

	for index, part := range parts {
		match := clusterRegexCompile.MatchString(part)
		if !match {
			log.Error("property " + ClusterServerAddr + " value is invalid: " + part)
			os.Exit(1)
		}
		clusterServers[index] = part
	}
	return clusterServers
}

func validateNumber(id, key string, opts []kv) any {
	clusterRegexCompile := regexp.MustCompile("\\d+")
	match := clusterRegexCompile.MatchString(id)
	if !match {
		log.Error("property " + NodeId + " value is invalid: " + id)
		os.Exit(1)
	}
	return id
}
