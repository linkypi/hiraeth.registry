package cluster

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/slot"
	"github.com/sirupsen/logrus"
	"sync"
)

type BaseCluster struct {
	*network.NetworkManager
	slotManager *slot.Manager
	Log         *logrus.Logger

	State    State
	StateMtx sync.Mutex

	Leader         *Leader
	Raft           *raft.Raft
	Config         *config.ClusterConfig
	nodeConfig     *config.NodeConfig
	ShutDownCh     chan struct{}
	SelfNode       *config.NodeInfo
	notifyLeaderCh chan bool

	// all cluster nodes configured in the configuration
	ClusterExpectedNodes map[string]*config.NodeInfo

	// The actual cluster nodes
	ClusterActualNodes map[string]*config.NodeInfo

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
	names := [...]string{"Initializing", "Active", "Transitioning", "StandBy", "Maintenance", "Unknown"}
	if s < 0 || s > State(len(names)-1) {
		return "Unknown"
	}
	return names[s]
}

func (b *BaseCluster) SetState(state State) {
	b.StateMtx.Lock()
	defer b.StateMtx.Unlock()
	b.State = state
}

func (b *BaseCluster) setNet(net *network.NetworkManager) {
	b.NetworkManager = net
}

func (b *BaseCluster) RaftExistNode(id string) bool {
	servers := b.Raft.GetConfiguration().Configuration().Servers
	for _, server := range servers {
		if string(server.ID) == id {
			return true
		}
	}
	return false
}

// GetOtherCandidateServers get other candidate servers, exclude self node
func (b *BaseCluster) GetOtherCandidateServers() []config.NodeInfo {
	var filtered []config.NodeInfo
	for _, node := range b.ClusterExpectedNodes {
		if node.IsCandidate {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (b *BaseCluster) GetOtherNodes(selfId string) []config.NodeInfo {
	var filtered = make([]config.NodeInfo, 0, 8)
	for _, node := range b.ClusterExpectedNodes {
		if node.Id != selfId {
			filtered = append(filtered, *node)
		}
	}
	return filtered
}

func (b *BaseCluster) MapToList(nodes map[string]config.NodeInfo) []config.NodeInfo {
	var result = make([]config.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node)
	}
	return result
}

func (b *BaseCluster) GetAddresses(nodes []config.NodeInfo) []string {
	var result = make([]string, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node.Addr)
	}
	return result
}

func (b *BaseCluster) CopyClusterNodes(nodes map[string]*config.NodeInfo) map[string]config.NodeInfo {
	var result = make(map[string]config.NodeInfo)
	for id, node := range nodes {
		result[id] = *node
	}
	return result
}

func (b *BaseCluster) GetNodeIdsIgnoreSelf(clusterServers []config.NodeInfo) []string {
	arr := make([]string, 0, len(clusterServers))
	for _, node := range clusterServers {
		if node.Id != b.SelfNode.Id {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (b *BaseCluster) CopyClusterNodesWithPtr(nodes map[string]config.NodeInfo) map[string]*config.NodeInfo {
	var result = make(map[string]*config.NodeInfo)
	for id, node := range nodes {
		result[id] = &node
	}
	return result
}

func (b *BaseCluster) getNodesByRaftServers(servers []raft.Server) []config.NodeInfo {
	var result = make([]config.NodeInfo, 0, len(servers))
	for _, server := range servers {
		for _, node := range b.ClusterExpectedNodes {
			if node.Addr == string(server.Address) {
				if server.Suffrage == raft.Voter {
					node.IsCandidate = true
				}
				result = append(result, *node)
				break
			}
		}
	}
	return result
}

func (b *BaseCluster) GetClusterExpectedNodeIds(clusterServers map[string]*config.NodeInfo) []string {
	arr := make([]string, 0, len(clusterServers))
	for _, node := range clusterServers {
		_, ok := b.Connections[node.Id]
		if ok {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (b *BaseCluster) RemoveNode(nodeId string) {
	for _, node := range b.ClusterExpectedNodes {
		if node.Id != nodeId {
			delete(b.ClusterExpectedNodes, node.Id)
		}
	}
}

func (b *BaseCluster) UpdateRemoteNode(remoteNode config.NodeInfo, selfNode config.NodeInfo, throwEx bool) error {

	if remoteNode.Id == selfNode.Id {
		msg := fmt.Sprintf("[cluster] the remote node id [%s][%s] conflicts with the current node id [%s][%s]",
			remoteNode.Id, remoteNode.Addr, selfNode.Id, selfNode.Addr)
		//b.Log.Warnf(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}

	node, ok := b.ClusterExpectedNodes[remoteNode.Id]
	if !ok {
		// it is possible that a node is missing from the cluster address
		// configuration in the node, or it may be a new node
		msg := fmt.Sprintf("[cluster] remote node [%s][%s] not found int cluster servers, add it to cluster.", remoteNode.Id, remoteNode.Addr)
		b.Log.Info(msg)
		node = &remoteNode
		b.ClusterExpectedNodes[remoteNode.Id] = node
	}
	if node.Addr != remoteNode.Addr {
		msg := fmt.Sprintf("[cluster] update remote node info failed, node id exist, but addr not match: %s, %s, %s",
			remoteNode.Id, node.Addr, remoteNode.Addr)
		b.Log.Error(msg)
		if throwEx {
			panic(msg)
		}
		return errors.New(msg)
	}
	node.IsCandidate = remoteNode.IsCandidate
	if node.IsCandidate {
		b.OtherCandidateNodes = append(b.OtherCandidateNodes, *node)
	}
	b.Log.Infof("[cluster] update remote node info: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (b *BaseCluster) IsLeader() bool {
	return b.Raft.State() == raft.Leader
}

func (b *BaseCluster) IsActive() bool {
	return b.State == Active
}
