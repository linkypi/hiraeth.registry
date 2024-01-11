package cluster

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/sirupsen/logrus"
	"sync"
)

type BaseCluster struct {
	*network.NetworkManager
	Log *logrus.Logger

	State    State
	StateMtx sync.Mutex

	Leader              *Leader
	Raft                *raft.Raft
	Config              *config.ClusterConfig
	ShutDownCh          chan struct{}
	SelfNode            *config.NodeInfo
	notifyLeaderCh      chan bool
	ClusterServers      map[string]*config.NodeInfo
	OtherCandidateNodes []config.NodeInfo
	joinCluster         bool
}

type State int

const (
	Initializing State = iota
	// Active The cluster is active and can be read and written to
	Active
	// Transitioning The cluster is transitioning to a new state
	// In this process, the cluster will resize the data shards, just
	// processes read requests and rejects write requests
	Transitioning
	// StandBy The cluster is in a standby state and can only be read
	// It's possible that the cluster is electing a leader
	StandBy
	// Maintenance The cluster is in a maintenance state and cannot be read or written
	Maintenance
	Unknown
)

func (s State) String() string {
	names := [...]string{"Active", "StandBy", "Maintenance", "Unknown"}
	if s < 0 || s > State(len(names)-1) {
		return "Unknown"
	}
	return names[s]
}

func (c *BaseCluster) SetState(state State) {
	c.StateMtx.Lock()
	defer c.StateMtx.Unlock()
	c.State = state
}

func (c *BaseCluster) setNet(net *network.NetworkManager) {
	c.NetworkManager = net
}

func (c *BaseCluster) RaftExistNode(id string) bool {
	servers := c.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		if string(server.ID) == id {
			return true
		}
	}
	return false
}

// GetOtherCandidateServers get other candidate servers, exclude self node
func (c *BaseCluster) GetOtherCandidateServers() []config.NodeInfo {
	var filtered []config.NodeInfo
	for _, node := range c.ClusterServers {
		if node.IsCandidate {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (c *BaseCluster) GetOtherNodes(selfId string) []config.NodeInfo {
	var filtered = make([]config.NodeInfo, 0, 8)
	for _, node := range c.ClusterServers {
		if node.Id != selfId {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (c *BaseCluster) RemoveNode(nodeId string) {
	for _, node := range c.ClusterServers {
		if node.Id != nodeId {
			delete(c.ClusterServers, node.Id)
		}
	}
}

func (c *BaseCluster) UpdateRemoteNode(remoteNode config.NodeInfo, selfNode config.NodeInfo, throwEx bool) error {

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

func (c *BaseCluster) IsLeader() bool {
	return c.Raft.State() == raft.Leader
}

func (c *BaseCluster) IsActive() bool {
	return c.State == Active
}
