package config

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"time"
)

type NodeInfo struct {
	Id                    string
	Ip                    string
	InternalPort          int
	Addr                  string
	externalHttpPort      int
	externalTcpPort       int
	IsCandidate           bool
	AutoJoinClusterEnable bool

	// Leader, Follower, Candidate
	State               NodeState
	LastStateUpdateTime time.Time
}

type NodeState string

const (
	Leader    NodeState = "Leader"
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
)

func (s NodeInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"id":                    s.Id,
		"ip":                    s.Ip,
		"internalPort":          s.InternalPort,
		"addr":                  s.Addr,
		"externalHttpPort":      s.externalHttpPort,
		"externalTcpPort":       s.externalTcpPort,
		"IsCandidate":           s.IsCandidate,
		"AutoJoinClusterEnable": s.AutoJoinClusterEnable,
	})
}

type kv struct {
	key string
	val any
}
type property struct {
	key      string
	propName string
	// if no data type is specified, reflection is used to
	// automatically convert to the corresponding data type
	dataType     string
	require      bool
	defaultVal   any
	parseHandler func(val, key string, ops []kv) any
	Options      []kv
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
	// LogLevel specify the log level, with the following values: debug, info, warn, error, fatal.
	// The default log level is info
	LogLevel = "log.level"
	// NumberOfReplicas To ensure that data is not lost, each shard will store several replica shards,
	// and replica shards can only be stored on other nodes. If the cluster has N nodes, the replica shards
	// can only be set to N-1 at most, but at least one is guaranteed
	NumberOfReplicas = "number.of.replicas"
)

type Config struct {
	NodeConfig    NodeConfig
	ClusterConfig ClusterConfig
	LogLevel      logrus.Level
	JoinCluster   bool
	StartupMode   StartUpMode
}

type NodeConfig struct {
	SelfNode          *NodeInfo
	DataDir           string
	LogDir            string
	HeartbeatInterval int
}

func (c NodeConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"dataDir":           c.DataDir,
		"logDir":            c.LogDir,
		"heartbeatInterval": c.HeartbeatInterval,
	})
}

type StartUpMode int

const (
	StandAlone StartUpMode = iota
	Cluster
)

type ClusterConfig struct {
	SelfNode            *NodeInfo
	OtherCandidateNodes []NodeInfo

	NumberOfReplicas         int
	ClusterServers           map[string]*NodeInfo
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int
	RaftElectionTimeout      int
	RaftHeartbeatTimeout     int
	AutoJoinClusterEnable    bool
	LogLevel                 logrus.Level
}

func (c ClusterConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"numberOfReplicas":         c.NumberOfReplicas,
		"clusterHeartbeatInterval": c.ClusterHeartbeatInterval,
		"clusterQuorumCount":       c.ClusterQuorumCount,
		"raftElectionTimeout":      c.RaftElectionTimeout,
		"raftHeartbeatTimeout":     c.RaftHeartbeatTimeout,
		"autoJoinClusterEnable":    c.AutoJoinClusterEnable,
		"logLevel":                 c.LogLevel,
	})
}

// Use reflection to set properties, all of which must be public
type internalConfig struct {
	NodeId           string
	NodeIp           string
	NodeInternalPort int
	ClientHttpPort   int
	ClientTcpPort    int

	NumberOfReplicas         int
	JoinCluster              bool
	StartupMode              StartUpMode
	IsCandidate              bool
	ClusterServers           []string
	ClusterHeartbeatInterval int
	ClusterQuorumCount       int
	AutoJoinClusterEnable    bool
	RaftElectionTimeout      int
	RaftHeartbeatTimeout     int

	DataDir  string
	LogDir   string
	LogLevel logrus.Level
}

var StartupModeProp = property{
	dataType:     "",
	propName:     "StartupMode",
	key:          StartupMode,
	require:      false,
	defaultVal:   StandAlone,
	parseHandler: ParseByDefaultOpts,
	Options:      []kv{{key: "cluster", val: Cluster}, {key: "stand-alone", val: StandAlone}},
}

var LogLevelProp = property{
	dataType:     "",
	propName:     "LogLevel",
	key:          LogLevel,
	require:      false,
	defaultVal:   "info",
	parseHandler: ParseByDefaultOpts,
	Options: []kv{{key: "info", val: logrus.InfoLevel}, {key: "debug", val: logrus.DebugLevel},
		{key: "fatal", val: logrus.FatalLevel}, {key: "error", val: logrus.ErrorLevel}, {key: "warn", val: logrus.WarnLevel},
	},
}

var NumberOfReplicasProp = property{
	dataType:     "int",
	propName:     "NumberOfReplicas",
	key:          NumberOfReplicas,
	require:      false,
	defaultVal:   1,
	parseHandler: validateNumber,
}

func init() {
	properties = append(properties, StartupModeProp)
	properties = append(properties, LogLevelProp)
	properties = append(properties, NumberOfReplicasProp)
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
	properties = append(properties, property{dataType: "int", propName: "ClientHttpPort", key: ClientHttpPort, require: false, defaultVal: 5042})
	properties = append(properties, property{dataType: "int", propName: "ClientTcpPort", key: ClientTcpPort, require: false, defaultVal: 5386})
	properties = append(properties, property{dataType: "string", propName: "DataDir", key: DataDir, require: false, defaultVal: "./data"})
	properties = append(properties, property{dataType: "string", propName: "LogDir", key: LogDir, require: false, defaultVal: "./log"})
}
