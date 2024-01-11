package cluster

import (
	"context"
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type Cluster struct {
	*BaseCluster
	transferMtx sync.Mutex
}

func NewCluster(clusterConfig *config.ClusterConfig, joinCluster bool, selfNode *config.NodeInfo,
	net *network.NetworkManager, shutDownCh chan struct{}, log *logrus.Logger) *Cluster {
	cluster := Cluster{
		BaseCluster: &BaseCluster{
			Log:            log,
			joinCluster:    joinCluster,
			State:          Initializing,
			Config:         clusterConfig,
			ShutDownCh:     shutDownCh,
			SelfNode:       selfNode,
			notifyLeaderCh: make(chan bool, 10),
			ClusterServers: clusterConfig.ClusterServers,
		},
	}
	cluster.setNet(net)
	return &cluster
}

func (c *Cluster) Start(dataDir string) {
	// try connecting to other candidate nodes
	go c.connectOtherCandidateNodes()

	// wait for the quorum nodes to be connected
	for len(c.Connections)+1 < c.Config.ClusterQuorumCount {
		time.Sleep(200 * time.Millisecond)
		select {
		case <-c.ShutDownCh:
			c.Shutdown()
			return
		default:
		}
	}

	// start the raft server
	c.startRaftNode(dataDir)

	go c.monitoringLeadershipTransfer()

	go c.CheckConnClosed(c.ShutDownCh, func(id string) {
		nodeInfo := c.ClusterServers[id]
		c.Log.Infof("node %s is disconnected, re-establish the connection: %s", nodeInfo.Id, nodeInfo.Addr)
		c.ConnectToNode(*nodeInfo)
	})

	if c.SelfNode.AutoJoinClusterEnable && c.joinCluster {
		go func() { c.joinToCluster() }()
	}
}

func (c *Cluster) monitoringLeadershipTransfer() {
	isDown := false
	for {
		select {
		// Only the leader node will receive the message
		case _ = <-c.Raft.LeaderCh():
			go c.transferLeaderShip()

		// Only the leader node will receive the message
		// Multiple guarantees to obtain leadership transfer notices
		case _ = <-c.notifyLeaderCh:
			go c.transferLeaderShip()

		case <-c.ShutDownCh:
			isDown = true
			break
		}
		if isDown {
			break
		}
	}
}

func (c *Cluster) transferLeaderShip() {

	oldLeader := c.Leader

	addr, nodeId := c.Raft.LeaderWithID()
	stats := c.Raft.Stats()
	term, _ := strconv.Atoi(stats["term"])

	//  If the leader is not changed, it will not be executed
	if oldLeader != nil && oldLeader.id == string(nodeId) && oldLeader.term == term {
		return
	}

	c.transferMtx.Lock()
	defer c.transferMtx.Unlock()
	// The first thing you need to do is set the cluster state to StandBy
	// In this case, the cluster only receives read requests and cannot process write requests
	c.SetState(Transitioning)

	jsonStr, _ := json.Marshal(stats)
	c.Log.Debugf("transfer leadership to %s, raft stats : %s", nodeId, jsonStr)

	c.UpdateLeader(term, string(nodeId), string(addr))
	c.Log.Infof("transfer leadership to %s:%s", nodeId, addr)

	// Notify the follower node that the leader has been transferred,
	// and the cluster no longer receives write requests and only processes read requests
	// After the data migration is complete, the cluster is back to Active
	go c.notifyLeaderShipTransfer(pb.TransferStatus_Transitioning)

	// Initialize the hash slot and shard the data
	// Or resize the data sharding
}

func (c *Cluster) joinToCluster() {
	connectedNodes := c.NetworkManager.GetConnectedNodes(c.ClusterServers)
	for _, node := range connectedNodes {
		rpcClient := c.NetworkManager.GetInterRpcClient(node.Id)
		request := pb.JoinClusterRequest{
			NodeId:                c.SelfNode.Id,
			NodeAddr:              c.SelfNode.Addr,
			AutoJoinClusterEnable: c.SelfNode.AutoJoinClusterEnable,
			IsCandidate:           c.SelfNode.IsCandidate,
		}
		_, err := rpcClient.JoinCluster(context.Background(), &request)
		if err != nil {
			c.Log.Errorf("failed to join cluster from remote node, id: %s, addr: %s, %s", node.Id, node.Addr, err.Error())
		}
	}
}
