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
	Log                 *logrus.Logger
	SelfNode            *config.NodeInfo
	ClusterServers      map[string]*config.NodeInfo
	OtherCandidateNodes []config.NodeInfo

	Config *config.ClusterConfig
	Raft   *raft.Raft
	net    *network.NetworkManager

	ShutDownCh     chan struct{}
	notifyLeaderCh chan bool
}

func NewCluster(clusterConfig *config.ClusterConfig, selfNode *config.NodeInfo,
	net *network.NetworkManager, shutDownCh chan struct{}, log *logrus.Logger) *Cluster {
	return &Cluster{
		Log:            log,
		net:            net,
		Config:         clusterConfig,
		SelfNode:       selfNode,
		ShutDownCh:     shutDownCh,
		ClusterServers: clusterConfig.ClusterServers,
	}
}

func (c *Cluster) ExistNode(id string) bool {
	_, ok := c.ClusterServers[id]
	return ok
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
	c.Log.Debugf("[cluster] update remote node info success: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (c *Cluster) AddNewNode(node *config.NodeInfo) {

	_, leaderId := c.Raft.LeaderWithID()
	// only the leader node has the permission to add nodes, otherwise the addition fails
	if raft.ServerID(c.SelfNode.Id) == leaderId {
		indexFur := c.Raft.AddVoter(
			raft.ServerID(node.Id),
			raft.ServerAddress(node.Addr),
			c.Raft.AppliedIndex(),
			3*time.Second,
		)

		err := indexFur.Error()
		if err != nil {
			c.Log.Errorf("[cluster] add new node to cluster failed, node id: %s, addr: %s. %v", node.Id, node.Addr, err)
			return
		}

		c.Log.Infof("[cluster] add new node to cluster success, node id: %s, addr: %s", node.Id, node.Addr)
		return
	}

	// forward the request to the leader node for addition
	c.Log.Infof("[cluster] forwarding the request to the leader node for addition, node id: %s, addr: %s", node.Id, node.Addr)

}
