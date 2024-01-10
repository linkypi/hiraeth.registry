package config

// server node Info
type NodeInfo struct {
	Id                    string
	Ip                    string
	InternalPort          int
	Addr                  string
	externalHttpPort      int
	externalTcpPort       int
	IsCandidate           bool
	AutoJoinClusterEnable bool
}

type property struct {
	key      string
	propName string
	// if no data type is specified, reflection is used to
	// automatically convert to the corresponding data type
	dataType     string
	require      bool
	defaultVal   any
	parseHandler func(val string, ops []string) any
	options      []string
}

const (
	NodeId            = "node.id"
	NodeIp            = "node.ip"
	NodeInternalPort  = "node.internal.port"
	IsCandidate       = "node.is.candidate"
	ClusterServerAddr = "cluster.server.addr"
	// ClusterHeartbeatInterval cluster node heart beat interval in seconds
	ClusterHeartbeatInterval = "cluster.heartbeat.interval"
	ClientHttpPort           = "client.http.port"
	ClientTcpPort            = "client.tcp.port"
	DataDir                  = "data.dir"
	LogDir                   = "log.dir"
	// ClusterQuorumCount specify the number of nodes required for cluster election
	ClusterQuorumCount = "cluster.quorum.count"
	// AutoJoinClusterEnable automatically join the cluster when starting up, default value is false
	AutoJoinClusterEnable = "auto.join.cluster.enable"
	// RaftHeartbeatTimeout Raft heartbeat timeout, unit is ms, default value is one second
	RaftHeartbeatTimeout = "raft.heartbeat.timeout"
	// RaftElectionTimeout Raft election timeout, unit is ms, default value is one second
	RaftElectionTimeout = "raft.election.timeout"
	// StartupMode there are two service startup modes, one is stand-alone mode and the other is cluster mode.
	// The default startup mode is stand-alone mode, which is used for local development and testing
	// and the cluster mode is used in the production environment. There are two options: stand-alone, cluster.
	StartupMode = "startup.mode"
)

type Config struct {
	NodeConfig    NodeConfig
	ClusterConfig ClusterConfig
}

type NodeConfig struct {
	SelfNode   *NodeInfo
	DataDir    string
	LogDir     string
	StandAlone bool
}

type StartUpMode int

const (
	StandAlone StartUpMode = iota
	Cluster
)

type ClusterConfig struct {
	SelfNode            *NodeInfo
	OtherCandidateNodes []NodeInfo

	StartupMode              StartUpMode
	ClusterServers           map[string]*NodeInfo
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int
	RaftElectionTimeout      int
	RaftHeartbeatTimeout     int
	AutoJoinClusterEnable    bool
}

type internalConfig struct {
	NodeId           string
	NodeIp           string
	NodeInternalPort int
	ClientHttpPort   int
	ClientTcpPort    int

	StartupMode              StartUpMode
	IsCandidate              bool
	ClusterServers           []string
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int
	AutoJoinClusterEnable    bool
	RaftElectionTimeout      int
	RaftHeartbeatTimeout     int

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
	properties = append(properties, property{
		dataType:   "bool",
		propName:   "AutoJoinClusterEnable",
		key:        AutoJoinClusterEnable,
		require:    false,
		defaultVal: false,
	})
	properties = append(properties, property{
		dataType:   "int",
		propName:   "RaftHeartbeatTimeout",
		key:        RaftHeartbeatTimeout,
		require:    false,
		defaultVal: 1000,
	})
	properties = append(properties, property{
		dataType:   "int",
		propName:   "RaftElectionTimeout",
		key:        RaftElectionTimeout,
		require:    false,
		defaultVal: 1000,
	})
	properties = append(properties, property{
		dataType:     "",
		propName:     "StartupMode",
		key:          StartupMode,
		require:      false,
		defaultVal:   "stand-alone",
		parseHandler: validateStartupMode,
		options:      []string{"cluster", "stand-alone"},
	})
	properties = append(properties, property{dataType: "int", propName: "ClientHttpPort", key: ClientHttpPort, require: false, defaultVal: 5042})
	properties = append(properties, property{dataType: "int", propName: "ClientTcpPort", key: ClientTcpPort, require: false, defaultVal: 5386})
	properties = append(properties, property{dataType: "string", propName: "DataDir", key: DataDir, require: false, defaultVal: "./data"})
	properties = append(properties, property{dataType: "string", propName: "LogDir", key: LogDir, require: false, defaultVal: "./log"})
}
