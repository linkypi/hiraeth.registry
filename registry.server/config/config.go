package config

import (
	"bufio"
	"github.com/linkypi/hiraeth.registry/common"
	"os"
	"reflect"
	"strconv"
	"strings"
)

var properties = make([]property, 0, 16)

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

func ParseConfig(filePath string) Config {
	configMap, err := loadConfig(filePath)
	if err != nil {
		common.Error("load config file failed", err)
		os.Exit(1)
	}

	interConfig := internalConfig{}
	for _, prop := range properties {
		val, exist := configMap[prop.key]
		if prop.require && (!exist || val == "") {
			common.Error("property " + prop.key + " cannot be empty.")
			os.Exit(1)
		}
		setInternalConfigProperty(&interConfig, prop, val)
	}

	if interConfig.StartupMode == Cluster && (len(interConfig.ClusterServers) < 2) {
		common.Error("the cluster address is not configured, it must be configured when starting with cluster mode")
		os.Exit(1)
	}

	nodeConfig, selfNode := buildNode(interConfig)
	clusterConfig := buildClusterConfig(selfNode, interConfig)

	config := Config{
		NodeConfig:    nodeConfig,
		ClusterConfig: clusterConfig,
		LogLevel:      interConfig.LogLevel,
		StartupMode:   interConfig.StartupMode,
		JoinCluster:   interConfig.JoinCluster,
	}
	return config
}

func buildNode(interConfig internalConfig) (NodeConfig, *NodeInfo) {
	nodeConfig := NodeConfig{}
	selfNode := &NodeInfo{
		Id:                    interConfig.NodeId,
		Ip:                    interConfig.NodeIp,
		Addr:                  interConfig.NodeIp + ":" + strconv.Itoa(interConfig.NodeInternalPort),
		IsCandidate:           interConfig.IsCandidate,
		ExternalHttpPort:      interConfig.ClientHttpPort,
		ExternalTcpPort:       interConfig.ClientTcpPort,
		InternalPort:          interConfig.NodeInternalPort,
		AutoJoinClusterEnable: interConfig.AutoJoinClusterEnable,
	}
	nodeConfig.SelfNode = selfNode
	nodeConfig.LogDir = interConfig.LogDir
	nodeConfig.DataDir = interConfig.DataDir
	nodeConfig.HeartbeatInterval = interConfig.ClusterHeartbeatInterval
	nodeConfig.ClientTcpPort = interConfig.ClientTcpPort
	nodeConfig.ClientHttpPort = interConfig.ClientHttpPort
	return nodeConfig, selfNode
}

func buildClusterConfig(selfNode *NodeInfo, interConfig internalConfig) ClusterConfig {
	clusterConfig := ClusterConfig{
		SelfNode:                           selfNode,
		ServiceInstanceRemoveTimeoutSec:    interConfig.ServiceInstanceRemoveTimeoutSec,
		ServiceInstanceUnhealthyTimeoutSec: interConfig.ServiceInstanceUnhealthyTimeoutSec,
		NumberOfReplicas:                   interConfig.NumberOfReplicas,
		ClusterHeartbeatInterval:           interConfig.ClusterHeartbeatInterval,
		ClusterQuorumCount:                 interConfig.ClusterQuorumCount,
		AutoJoinClusterEnable:              interConfig.AutoJoinClusterEnable,
		RaftHeartbeatTimeout:               interConfig.RaftHeartbeatTimeout,
		RaftElectionTimeout:                interConfig.RaftElectionTimeout,
		LogLevel:                           interConfig.LogLevel,
	}

	clusterConfig.ClusterServers = make(map[string]*NodeInfo)
	clusterConfig.ClusterServers[selfNode.Id] = selfNode
	for _, server := range interConfig.ClusterServers {
		parts := strings.Split(server, ":")
		id := parts[0]
		ip := parts[1]
		port, err := strconv.Atoi(parts[2])
		if err != nil {
			common.Error("parse candidate server port failed: "+server, err)
			os.Exit(1)
		}
		addr := parts[1] + ":" + parts[2]
		if addr == selfNode.Addr {
			continue
		}
		// The property isCandidate of node can only be known through rpc communication, so isCandidate is not assigned here
		node := NodeInfo{
			Id:           id,
			Ip:           ip,
			Addr:         ip + ":" + strconv.Itoa(port),
			InternalPort: port,
		}
		clusterConfig.ClusterServers[id] = &node
	}
	return clusterConfig
}

func setInternalConfigProperty(config *internalConfig, prop property, val string) {
	elems := reflect.ValueOf(config).Elem()
	setConfigProperty(elems, prop, val)
}
func SetClusterConfigProperty(config *ClusterConfig, prop property, val string) {
	elems := reflect.ValueOf(config).Elem()
	setConfigProperty(elems, prop, val)
}
func SetConfigProperty(config *Config, prop property, val string) {
	elems := reflect.ValueOf(config).Elem()
	setConfigProperty(elems, prop, val)
}

func setConfigProperty(elems reflect.Value, prop property, val string) {
	field := elems.FieldByName(prop.propName)
	defer common.PrintStackTrace()
	//defer func() {
	//	if r := recover(); r != nil {
	//		common.Errorf("config value of property [%s] is invalid: %s, %v", prop.propName, val, r)
	//		os.Exit(1)
	//	}
	//}()
	if field.IsValid() && field.CanSet() {
		if field.Kind() == reflect.Array && prop.parseHandler != nil {
			newValues := prop.parseHandler(val, prop.key, prop.Options)
			field.Set(reflect.ValueOf(newValues))
			return
		}

		switch prop.dataType {
		case "string":
			if val == "" {
				common.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				field.SetString(prop.defaultVal.(string))
				return
			}
			if prop.parseHandler != nil {
				v := prop.parseHandler(val, prop.key, prop.Options)
				field.Set(reflect.ValueOf(v))
				return
			}
			field.SetString(val)

		case "int":
			strVal := val
			if val == "" {
				common.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				ret := reflect.ValueOf(prop.defaultVal)
				field.Set(ret.Convert(field.Type()))
				return
			}
			if prop.parseHandler != nil {
				prop.parseHandler(val, prop.key, prop.Options)
			}
			intVal, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				common.Errorf("property %s default value is invalid: %s", prop.key, strVal)
				os.Exit(1)
			}
			ret := reflect.ValueOf(intVal)
			field.Set(ret.Convert(field.Type()))

		case "bool":
			strVal := val
			if val == "" {
				common.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				field.SetBool(prop.defaultVal.(bool))
				return
			}
			if prop.parseHandler != nil {
				v := prop.parseHandler(val, prop.key, prop.Options)
				field.Set(reflect.ValueOf(v))
				return
			}
			bVal, err := strconv.ParseBool(strVal)
			if err != nil {
				common.Errorf("property %s default value is invalid: %s", prop.key, strVal)
				os.Exit(1)
			}
			field.SetBool(bVal)
		default:
			if val == "" {
				common.Debugf("property not specified %s, use default value instead: %s", prop.propName, prop.defaultVal)
				valueOf := reflect.ValueOf(prop.defaultVal)
				field.Set(valueOf.Convert(field.Type()))
				return
			}
			if prop.parseHandler != nil {
				v := prop.parseHandler(val, prop.key, prop.Options)
				field.Set(reflect.ValueOf(v))
				return
			}
			ret := reflect.ValueOf(val)
			field.Set(ret.Convert(field.Type()))
		}
	}
}

//func setDefaultVal(filed reflect.Value, dataType string, val any) {
//	t := reflect.TypeOf(val)
//	if t.Kind() == reflect.String {
//		if dataType == "int" {
//			intVal, err := strconv.Atoi(val.(string))
//			if err != nil {
//				common.Errorf("property %s default value is invalid: %s", prop.key, val)
//				os.Exit(1)
//			}
//			filed.SetInt(intVal)
//		}
//		filed.SetString(val.(string))
//		return
//	}
//	if t.Kind() == reflect.Ptr {
//		t = t.Elem()
//	}
//	switch t.Kind() {
//	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
//		val = int64(0)
//	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
//		val = uint64(0)
//	}
//}
