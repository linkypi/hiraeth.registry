package config

import (
	"bufio"
	"github.com/sirupsen/logrus"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type ServerConfig struct {
	CurrentNode              node
	CandidateNodeMap         map[int]node
	OtherCandidateNodes      []node
	DataDir                  string
	LogDir                   string
	ClusterHeartbeatInterval int
}

func (sc *ServerConfig) UpdateNode(nodeId int, nodeIp string, nodeInternalPort int) {
	addr := nodeIp + ":" + strconv.Itoa(nodeInternalPort)
	for _, candidateNode := range sc.OtherCandidateNodes {
		if candidateNode.Addr == addr {
			candidateNode.Id = nodeId
			sc.CandidateNodeMap[nodeId] = candidateNode
			log.Debugf("update node success: %d, %s", nodeId, addr)
			return
		}
	}
}

type internalConfig struct {
	NodeId           int
	NodeIp           string
	NodeInternalPort int
	ClientHttpPort   int
	ClientTcpPort    int

	IsControllerCandidate    bool
	CandidateServers         []string
	ClusterHeartbeatInterval int

	DataDir string
	LogDir  string
}

// server node info
type node struct {
	Id               int
	Ip               string
	InternalPort     int
	Addr             string
	externalHttpPort int
	externalTcpPort  int
	isCandidate      bool
}

type property struct {
	key          string
	propName     string
	dataType     string
	require      bool
	defaultVal   any
	parseHandler func(val string) any
}

const NODE_ID = "node.id"
const NODE_IP = "node.ip"
const NODE_INTERNAL_PORT = "node.internal.port"
const IS_CONTROLLER_CANDIDATE = "is.controller.candidate"
const CONTROLLER_CANDIDATE_SERVERS = "controller.candidate.servers"
const CLIENT_HTTP_PORT = "client.http.port"
const CLIENT_TCP_PORT = "client.tcp.port"
const DATA_DIR = "data.dir"
const LOG_DIR = "log.dir"
const CLUSTER_HEARTBEAT_INTERVAL = "cluster.heartbeat.interval"

var properties = make([]property, 0, 16)
var log *logrus.Logger

func init() {
	properties = append(properties, property{
		dataType:     "int",
		propName:     "NodeId",
		key:          NODE_ID,
		require:      true,
		defaultVal:   nil,
		parseHandler: validateNumber,
	})
	properties = append(properties, property{
		dataType:     "string",
		propName:     "NodeIp",
		key:          NODE_IP,
		require:      true,
		defaultVal:   nil,
		parseHandler: validateIP,
	})
	properties = append(properties, property{
		dataType:   "int",
		propName:   "NodeInternalPort",
		key:        NODE_INTERNAL_PORT,
		require:    false,
		defaultVal: 2661})
	properties = append(properties, property{
		dataType:   "bool",
		propName:   "IsControllerCandidate",
		key:        IS_CONTROLLER_CANDIDATE,
		require:    false,
		defaultVal: true})
	properties = append(properties, property{
		dataType:     "string",
		propName:     "CandidateServers",
		key:          CONTROLLER_CANDIDATE_SERVERS,
		parseHandler: parseCtrlCandidateServers,
		require:      true,
		defaultVal:   nil})
	properties = append(properties, property{
		dataType:     "int",
		propName:     "ClusterHeartbeatInterval",
		key:          CLUSTER_HEARTBEAT_INTERVAL,
		parseHandler: validateNumber,
		require:      false,
		defaultVal:   5})
	properties = append(properties, property{dataType: "int", propName: "ClientHttpPort", key: CLIENT_HTTP_PORT, require: false, defaultVal: 5042})
	properties = append(properties, property{dataType: "int", propName: "ClientTcpPort", key: CLIENT_TCP_PORT, require: false, defaultVal: 5386})
	properties = append(properties, property{dataType: "string", propName: "DataDir", key: DATA_DIR, require: false, defaultVal: "./data"})
	properties = append(properties, property{dataType: "string", propName: "LogDir", key: LOG_DIR, require: false, defaultVal: "./log"})
}

func validateNumber(id string) any {
	clusterRegexCompile := regexp.MustCompile("d+")
	match := clusterRegexCompile.MatchString(id)
	if !match {

		log.Error("property " + NODE_ID + " value is invalid: " + id)
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

func ParseConfig(filePath string, logger *logrus.Logger) ServerConfig {
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

	config := ServerConfig{}
	currentNode := node{
		Id:               interConfig.NodeId,
		Ip:               interConfig.NodeIp,
		Addr:             interConfig.NodeIp + ":" + strconv.Itoa(interConfig.NodeInternalPort),
		isCandidate:      interConfig.IsControllerCandidate,
		externalHttpPort: interConfig.ClientHttpPort,
		externalTcpPort:  interConfig.ClientTcpPort,
		InternalPort:     interConfig.NodeInternalPort,
	}
	config.CurrentNode = currentNode
	config.CandidateNodeMap = make(map[int]node)
	config.CandidateNodeMap[currentNode.Id] = currentNode
	config.LogDir = interConfig.LogDir
	config.DataDir = interConfig.DataDir
	config.ClusterHeartbeatInterval = interConfig.ClusterHeartbeatInterval

	config.OtherCandidateNodes = make([]node, 0, 2)
	for _, server := range interConfig.CandidateServers {
		if server == currentNode.Addr {
			continue
		}
		parts := strings.Split(server, ":")
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Error("parse candidate server port failed: "+server, err)
			os.Exit(1)
		}
		node := node{
			Ip:           parts[0],
			Addr:         parts[0] + ":" + strconv.Itoa(port),
			InternalPort: port,
			isCandidate:  true,
		}
		config.OtherCandidateNodes = append(config.OtherCandidateNodes, node)
	}

	//sort.Slice(config.OtherCandidateNodes, func(i, j int) bool {
	//	addrA := config.OtherCandidateNodes[i].addr
	//	addrB := config.OtherCandidateNodes[j].addr
	//	return murmur3.Sum64([]byte(addrA)) < murmur3.Sum64([]byte(addrB))
	//})

	return config
}

func setConfigProperty(config *internalConfig, prop property, val string) {
	elems := reflect.ValueOf(config).Elem()
	field := elems.FieldByName(prop.propName)
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
				v := prop.parseHandler(val)
				field.Set(reflect.ValueOf(v))
				return
			}
			intVal, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				log.Errorf("property %s default value is invalid: %s", prop.key, strVal)
				os.Exit(1)
			}
			field.SetInt(intVal)

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
		}
	}
}

func parseCtrlCandidateServers(ctrlCandidateServers string) any {
	parts := strings.Split(ctrlCandidateServers, ",")
	controllerCandidateServers := make([]string, len(parts))

	clusterRegexCompile := regexp.MustCompile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)")

	for index, part := range parts {
		match := clusterRegexCompile.MatchString(part)
		if !match {
			log.Error("property " + CONTROLLER_CANDIDATE_SERVERS + " value is invalid: " + part)
			os.Exit(1)
		}
		controllerCandidateServers[index] = part
	}
	return controllerCandidateServers
}

func validateIP(ip string) any {
	clusterRegexCompile := regexp.MustCompile("\\d+\\.\\d+\\.\\d+\\.\\d+")
	match := clusterRegexCompile.MatchString(ip)
	if !match {
		log.Error("property " + NODE_IP + " value is invalid: " + ip)
		os.Exit(1)
	}
	return ip
}
