package config

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

func (nc *NodeConfig) UpdateRemoteNode(remoteNode NodeInfo, throwEx bool) {

	if remoteNode.Id == nc.SelfNode.Id {
		msg := fmt.Sprintf("the remote node id [%s][%s] conflicts with the current node id [%s][%s]",
			remoteNode.Id, remoteNode.Addr, nc.SelfNode.Id, nc.SelfNode.Addr)
		log.Error(msg)
		if throwEx {
			panic(msg)
		}
		return
	}

	node, ok := nc.ClusterServers[remoteNode.Id]
	if !ok {
		// it is possible that a node is missing from the cluster address
		// configuration in the node, or it may be a new node
		msg := fmt.Sprintf("remote node [%s][%s] not found int cluster servers, add it to cluster.", remoteNode.Id, remoteNode.Addr)
		log.Info(msg)
		node = remoteNode
		nc.ClusterServers[remoteNode.Id] = remoteNode
	}
	if node.Addr != remoteNode.Addr {
		msg := fmt.Sprintf("update remote node info failed, node id exist, but addr not match: %s, %s, %s",
			remoteNode.Id, node.Addr, remoteNode.Addr)
		log.Error(msg)
		if throwEx {
			panic(msg)
		}
		return
	}
	node.IsCandidate = remoteNode.IsCandidate
	if node.IsCandidate {
		nc.OtherCandidateNodes = append(nc.OtherCandidateNodes, node)
	}
	log.Debugf("update remote node info success: %s, %s", remoteNode.Id, remoteNode.Addr)
}

// get other candidate servers, exclude self node
func (nc *NodeConfig) GetOtherCandidateServers() []NodeInfo {
	var filtered []NodeInfo
	for _, node := range nc.ClusterServers {
		if node.IsCandidate {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

func (nc *NodeConfig) GetOtherNodes() []NodeInfo {
	var filtered = make([]NodeInfo, 0, 8)
	for _, node := range nc.ClusterServers {
		if node.Id != nc.SelfNode.Id {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

var properties = make([]property, 0, 16)
var log *logrus.Logger

func validateNumber(id string) any {
	clusterRegexCompile := regexp.MustCompile("\\d+")
	match := clusterRegexCompile.MatchString(id)
	if !match {
		log.Error("property " + NodeId + " value is invalid: " + id)
		os.Exit(1)
	}
	return id
}

// loadConfig reads a .conf file and parses its contents into a map
func loadConfig(filePath string) (map[string]string, error) {
	props := make(map[string]string)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue // skip comments and empty lines
		}

		if idx := strings.Index(line, "="); idx != -1 {
			key := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
			props[key] = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return props, nil
}

func ParseConfig(filePath string, logger *logrus.Logger) NodeConfig {
	log = logger
	configMap, err := loadConfig(filePath)
	if err != nil {
		log.Error("load config file failed", err)
		os.Exit(1)
	}

	interConfig := internalConfig{}
	for _, prop := range properties {
		val, exist := configMap[prop.key]
		if prop.require && (!exist || val == "") {
			log.Error("property " + prop.key + " cannot be empty.")
			os.Exit(1)
		}
		setConfigProperty(&interConfig, prop, val)
	}

	config := NodeConfig{}
	selfNode := NodeInfo{
		Id:               interConfig.NodeId,
		Ip:               interConfig.NodeIp,
		Addr:             interConfig.NodeIp + ":" + strconv.Itoa(interConfig.NodeInternalPort),
		IsCandidate:      interConfig.IsCandidate,
		externalHttpPort: interConfig.ClientHttpPort,
		externalTcpPort:  interConfig.ClientTcpPort,
		InternalPort:     interConfig.NodeInternalPort,
	}
	config.SelfNode = selfNode
	config.ClusterServers = make(map[string]NodeInfo)
	config.ClusterServers[selfNode.Id] = selfNode

	config.LogDir = interConfig.LogDir
	config.DataDir = interConfig.DataDir
	config.ClusterHeartbeatInterval = interConfig.ClusterHeartbeatInterval
	config.ClusterQuorumCount = interConfig.ClusterQuorumCount

	for _, server := range interConfig.ClusterServers {
		if server == selfNode.Addr {
			continue
		}
		parts := strings.Split(server, ":")
		id := parts[0]
		ip := parts[1]
		port, err := strconv.Atoi(parts[2])
		if err != nil {
			log.Error("parse candidate server port failed: "+server, err)
			os.Exit(1)
		}
		// The property isCandidate of node can only be known through rpc communication, so isCandidate is not assigned here
		node := NodeInfo{
			Id:           id,
			Ip:           ip,
			Addr:         ip + ":" + strconv.Itoa(port),
			InternalPort: port,
		}
		config.ClusterServers[id] = node
	}

	return config
}

func setConfigProperty(config *internalConfig, prop property, val string) {
	elems := reflect.ValueOf(config).Elem()
	field := elems.FieldByName(prop.propName)
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("config value of property [%s] is invalid: %s, %v", prop.propName, val, r)
			os.Exit(1)
		}
	}()
	if field.IsValid() && field.CanSet() {
		if field.Kind() == reflect.Array && prop.parseHandler != nil {
			newValues := prop.parseHandler(val)
			field.Set(reflect.ValueOf(newValues))
			return
		}

		switch prop.dataType {
		case "string":
			if val == "" {
				log.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				field.SetString(prop.defaultVal.(string))
				return
			}
			if prop.parseHandler != nil {
				v := prop.parseHandler(val)
				field.Set(reflect.ValueOf(v))
				return
			}
			field.SetString(val)

		case "int":
			strVal := val
			if val == "" {
				log.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				var result int64 = int64(prop.defaultVal.(int))
				field.SetInt(result)
				return
			}
			if prop.parseHandler != nil {
				prop.parseHandler(val)
			}
			intVal, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				log.Errorf("property %s default value is invalid: %s", prop.key, strVal)
				os.Exit(1)
			}
			ret := reflect.ValueOf(intVal)
			field.Set(ret.Convert(field.Type()))

		case "bool":
			strVal := val
			if val == "" {
				log.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				field.SetBool(prop.defaultVal.(bool))
				return
			}
			if prop.parseHandler != nil {
				v := prop.parseHandler(val)
				field.Set(reflect.ValueOf(v))
				return
			}
			bVal, err := strconv.ParseBool(strVal)
			if err != nil {
				log.Errorf("property %s default value is invalid: %s", prop.key, strVal)
				os.Exit(1)
			}
			field.SetBool(bVal)
		default:
			ret := reflect.ValueOf(val)
			field.Set(ret.Convert(field.Type()))
		}
	}
}

func parseClusterServers(ctrlCandidateServers string) any {
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

func validateIP(ip string) any {
	clusterRegexCompile := regexp.MustCompile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	match := clusterRegexCompile.MatchString(ip)
	if !match {
		log.Error("property " + NodeIp + " value is invalid: " + ip)
		os.Exit(1)
	}
	return ip
}
