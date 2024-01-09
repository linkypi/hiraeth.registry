package config

// server node Info
type NodeInfo struct {
	Id               string
	Ip               string
	InternalPort     int
	Addr             string
	externalHttpPort int
	externalTcpPort  int
	IsCandidate      bool
}

type property struct {
	key          string
	propName     string
	dataType     string
	require      bool
	defaultVal   any
	parseHandler func(val string) any
}

const (
	NodeId                   = "node.id"
	NodeIp                   = "node.ip"
	NodeInternalPort         = "node.internal.port"
	IsCandidate              = "node.is.candidate"
	ClusterServerAddr        = "cluster.server.addr"
	ClusterHeartbeatInterval = "cluster.heartbeat.interval"
	ClientHttpPort           = "client.http.port"
	ClientTcpPort            = "client.tcp.port"
	DataDir                  = "data.dir"
	LogDir                   = "log.dir"
	ClusterQuorumCount       = "cluster.quorum.count"
)

type NodeConfig struct {
	SelfNode NodeInfo
	// cluster server node info, include candidate node and observer node
	ClusterServers           map[string]NodeInfo
	OtherCandidateNodes      []NodeInfo
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int
	DataDir                  string
	LogDir                   string
	StandAlone               bool
}

type internalConfig struct {
	NodeId           string
	NodeIp           string
	NodeInternalPort int
	ClientHttpPort   int
	ClientTcpPort    int

	IsCandidate              bool
	ClusterServers           []string
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int

	DataDir string
	LogDir  string
}

func init() {
	properties = append(properties, property{
		dataType:     "string",
		propName:     "NodeId",
		key:          NodeId,
		require:      true,
		defaultVal:   nil,
		parseHandler: validateNumber,
	})
	properties = append(properties, property{
		dataType:     "string",
		propName:     "NodeIp",
		key:          NodeIp,
		require:      true,
		defaultVal:   nil,
		parseHandler: validateIP,
	})
	properties = append(properties, property{
		dataType:     "int",
		propName:     "NodeInternalPort",
		key:          NodeInternalPort,
		require:      false,
		parseHandler: validateNumber,
		defaultVal:   2661})
	properties = append(properties, property{
		dataType:   "bool",
		propName:   "IsCandidate",
		key:        IsCandidate,
		require:    false,
		defaultVal: true})
	properties = append(properties, property{
		dataType:     "string",
		propName:     "ClusterServers",
		key:          ClusterServerAddr,
		parseHandler: parseClusterServers,
		require:      true,
		defaultVal:   nil})
	properties = append(properties, property{
		dataType:     "int",
		propName:     "ClusterHeartbeatInterval",
		key:          ClusterHeartbeatInterval,
		parseHandler: validateNumber,
		require:      false,
		defaultVal:   5})
	properties = append(properties, property{
		dataType:     "int",
		propName:     "ClusterQuorumCount",
		key:          ClusterQuorumCount,
		require:      true,
		defaultVal:   nil,
		parseHandler: validateNumber,
	})
	properties = append(properties, property{dataType: "int", propName: "ClientHttpPort", key: ClientHttpPort, require: false, defaultVal: 5042})
	properties = append(properties, property{dataType: "int", propName: "ClientTcpPort", key: ClientTcpPort, require: false, defaultVal: 5386})
	properties = append(properties, property{dataType: "string", propName: "DataDir", key: DataDir, require: false, defaultVal: "./data"})
	properties = append(properties, property{dataType: "string", propName: "LogDir", key: LogDir, require: false, defaultVal: "./log"})
}
