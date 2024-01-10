package cluster

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/sirupsen/logrus"
	"time"
)

type Cluster struct {
	*network.NetworkManager
	Log                 *logrus.Logger
	State               State
	SelfNode            *config.NodeInfo
	Leader              config.NodeInfo
	ClusterServers      map[string]*config.NodeInfo
	OtherCandidateNodes []config.NodeInfo

	Raft           *raft.Raft
	Config         *config.ClusterConfig
	ShutDownCh     chan struct{}
	notifyLeaderCh chan bool
}

type State int

const (
	// Active The cluster is active and can be read and written to
	Active State = iota
	// StandBy The cluster is in a standby state and can only be read
	// It's possible that the cluster is electing a leader
	StandBy
	// Maintenance The cluster is in a maintenance state and cannot be read or written
	Maintenance
	Unknown
)

func NewCluster(clusterConfig *config.ClusterConfig, selfNode *config.NodeInfo,
	net *network.NetworkManager, shutDownCh chan struct{}, log *logrus.Logger) *Cluster {
	cluster := Cluster{
		Log:            log,
		State:          StandBy,
		Config:         clusterConfig,
		SelfNode:       selfNode,
		ShutDownCh:     shutDownCh,
		notifyLeaderCh: make(chan bool, 10),
		ClusterServers: clusterConfig.ClusterServers,
	}
	cluster.setNet(net)
	return &cluster
}

func (c *Cluster) setNet(net *network.NetworkManager) {
	c.NetworkManager = net
}

func (c *Cluster) RaftExistNode(id string) bool {
	servers := c.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		if string(server.ID) == id {
			return true
		}
	}
	return false
}

// GetOtherCandidateServers get other candidate servers, exclude self node
func (c *Cluster) GetOtherCandidateServers() []config.NodeInfo {
	var filtered []config.NodeInfo
	for _, node := range c.ClusterServers {
		if node.IsCandidate {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (c *Cluster) GetOtherNodes(selfId string) []config.NodeInfo {
	var filtered = make([]config.NodeInfo, 0, 8)
	for _, node := range c.ClusterServers {
		if node.Id != selfId {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (c *Cluster) UpdateRemoteNode(remoteNode config.NodeInfo, selfNode config.NodeInfo, throwEx bool) error {

	if remoteNode.Id == selfNode.Id {
		msg := fmt.Sprintf("[cluster] the remote node id [%s][%s] conflicts with the current node id [%s][%s]",
			remoteNode.Id, remoteNode.Addr, selfNode.Id, selfNode.Addr)
		c.Log.Error(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}

	node, ok := c.ClusterServers[remoteNode.Id]
	if !ok {
		// it is possible that a node is missing from the cluster address
		// configuration in the node, or it may be a new node
		msg := fmt.Sprintf("[cluster] remote node [%s][%s] not found int cluster servers, add it to cluster.", remoteNode.Id, remoteNode.Addr)
		c.Log.Info(msg)
		node = &remoteNode
		c.ClusterServers[remoteNode.Id] = node
	}
	if node.Addr != remoteNode.Addr {
		msg := fmt.Sprintf("[cluster] update remote node info failed, node id exist, but addr not match: %s, %s, %s",
			remoteNode.Id, node.Addr, remoteNode.Addr)
		c.Log.Error(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}
	node.IsCandidate = remoteNode.IsCandidate
	if node.IsCandidate {
		c.OtherCandidateNodes = append(c.OtherCandidateNodes, *node)
	}
	c.Log.Infof("[cluster] update remote node info: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (c *Cluster) isLeader() bool {
	return c.Raft.State() == raft.Leader
}

func (c *Cluster) AddNewNode(remoteNode *config.NodeInfo) {

	if c.Raft == nil {
		c.Log.Errorf("[cluster] raft is nil, can not add new node, node id: %s, node addr: %s", remoteNode.Id, remoteNode.Addr)
		return
	}
	// Check whether the connection of the new node exists, and if not, establish the connection first
	if !c.ExistConn(remoteNode.Id) {
		c.connectToNode(*remoteNode)
	}
	// only the leader node has the permission to add nodes, otherwise the addition fails
	if c.isLeader() {
		var indexFur raft.IndexFuture
		if remoteNode.IsCandidate {
			indexFur = c.Raft.AddVoter(
				raft.ServerID(remoteNode.Id),
				raft.ServerAddress(remoteNode.Addr),
				0,
				3*time.Second,
			)
		} else {
			indexFur = c.Raft.AddNonvoter(
				raft.ServerID(remoteNode.Id),
				raft.ServerAddress(remoteNode.Addr),
				0,
				3*time.Second,
			)
		}

		err := indexFur.Error()
		if err != nil {
			// 稍后重试, 有可能配置
			// add later , configuration changed since 30 (latest is 1)
			c.Log.Errorf("[cluster] add new node to cluster failed, node id: %s, addr: %s. %v", remoteNode.Id, remoteNode.Addr, err)
			return
		}

		c.Log.Infof("[cluster] add new node to cluster success, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
		return
	}
}

func (c *Cluster) UpdateLeader(id, addr string) {
	leader := config.NodeInfo{
		Id:   id,
		Addr: addr,
	}
	c.Leader = leader
	c.State = Active
}
