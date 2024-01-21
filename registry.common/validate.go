package common

import (
	"regexp"
	"strings"
)

func ParseClusterServers(addrs string) ([]string, bool) {
	parts := strings.Split(addrs, ",")
	clusterServers := make([]string, len(parts))

	clusterRegexCompile := regexp.MustCompile("(\\w+)\\:((\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.){3}(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\:(\\d{3,5})$")

	for index, part := range parts {
		match := clusterRegexCompile.MatchString(part)
		if !match {
			return nil, false
		}
		clusterServers[index] = part
	}
	return clusterServers, true
}

func ParseIpPort(addrs string) ([]string, bool) {
	parts := strings.Split(addrs, ",")
	clusterServers := make([]string, len(parts))

	clusterRegexCompile := regexp.MustCompile("((\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.){3}(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\:(\\d{3,5})$")

	for index, part := range parts {
		match := clusterRegexCompile.MatchString(part)
		if !match {
			return nil, false
		}
		clusterServers[index] = part
	}
	return clusterServers, true
}
