package config

import (
	"os"
	"regexp"
	"strings"
)

func validateIP(ip string, opts []string) any {
	clusterRegexCompile := regexp.MustCompile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	match := clusterRegexCompile.MatchString(ip)
	if !match {
		log.Error("property " + NodeIp + " value is invalid: " + ip)
		os.Exit(1)
	}
	return ip
}

// 可能的选项
func validateStartupMode(mode string, opts []string) any {

	if opts == nil {
		log.Error("property " + StartupMode + "'s options must not be empty. ")
		os.Exit(1)
	}
	exist := false
	for _, opt := range opts {
		if mode == opt {
			exist = true
			break
		}
	}
	if !exist {
		log.Errorf("property %s's value doesn't match the elements provided in options: %s", StartupMode, mode)
		os.Exit(1)
	}
	return mode
}

func parseClusterServers(ctrlCandidateServers string, opts []string) any {
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

func validateNumber(id string, opts []string) any {
	clusterRegexCompile := regexp.MustCompile("\\d+")
	match := clusterRegexCompile.MatchString(id)
	if !match {
		log.Error("property " + NodeId + " value is invalid: " + id)
		os.Exit(1)
	}
	return id
}
