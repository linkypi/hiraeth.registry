package cluster

import (
	"context"
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"strconv"
	"sync"
	"time"
)

type Cluster struct {
	*BaseCluster
	transferMtx sync.Mutex
}

const (
	MetaDataFileName = "/metadata.json"
	TermKey          = "term"
)

func NewCluster(conf *config.Config, selfNode *config.NodeInfo, slotManager *slot.Manager,
	net *network.Manager, shutDownCh chan struct{}) *Cluster {

	cluster := Cluster{
		BaseCluster: NewBaseCluster(conf, selfNode, slotManager, net, shutDownCh),
	}

	for id, node := range conf.ClusterConfig.ClusterServers {
		cluster.ClusterActualNodes.Store(id, node)
		cluster.ClusterExpectedNodes.Store(id, node)
	}
	cluster.setNet(net)
	return &cluster
}

func (c *Cluster) Start(dataDir string) {
	// try connecting to other candidate nodes
	go c.connectOtherCandidateNodes()

	// wait for the quorum nodes to be connected
	for len(c.Connections)+1 < c.Config.ClusterQuorumCount {
		select {
		case <-c.ShutDownCh:
			c.Shutdown()
			return
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	// start the raft server
	c.startRaftNode(dataDir)

	go c.monitoringClusterState()

	go c.CheckConnClosed(c.ShutDownCh, func(id string) {
		node, ok := c.ClusterActualNodes.Load(id)
		if !ok {
			common.Errorf("node %s is not found in the cluster", id)
			return
		}
		nodeInfo := node.(*config.NodeInfo)
		common.Infof("node %s is disconnected, re-establish the connection: %s", nodeInfo.Id, nodeInfo.Addr)
		c.ConnectToNode(nodeInfo)
	})

	if c.SelfNode.AutoJoinClusterEnable && c.joinCluster {
		go func() { c.joinToCluster() }()
	}
}

func (c *Cluster) handleClusterDowntime() {

	if c.State == Down {
		return
	}

	common.Errorf("cluster is down or the network partitioned")
	c.SetState(Down)
}

func (c *Cluster) monitoringClusterState() {
	detectDownTimes := 1
	c.lastStateTime = time.Now()
	for {
		select {
		// only be notified when a new leader is generated
		case _ = <-c.Raft.LeaderCh():
			_, leaderId := c.Raft.LeaderWithID()
			// When the cluster goes down, the leaderId may be empty
			if leaderId != "" {
				go c.transferLeaderShip()
			}

		case <-c.ShutDownCh:
			return
		default:
			// The follower node can only check the cluster health status in this way
			c.detectClusterState(&detectDownTimes)
			time.Sleep(time.Millisecond * 500)
		}
	}
}

func (c *Cluster) detectClusterState(detectDownTimes *int) {
	stats := c.Raft.Stats()
	state := stats["state"]

	seconds := time.Now().Sub(c.lastStateTime).Seconds()
	if seconds > 30 {
		common.Debugf(" node state [%s]", state)
		c.lastStateTime = time.Now()
	}

	if state != string(c.SelfNode.State) {
		c.SelfNode.State = config.NodeState(state)
		c.SelfNode.LastStateUpdateTime = time.Now()
	} else {
		// If the current node has become a candidate node and the duration
		// has exceeded one election cycles, detect three times. If the conditions are met, the cluster is down
		duration := time.Now().Sub(c.SelfNode.LastStateUpdateTime).Milliseconds()
		if c.SelfNode.State == config.Candidate && duration > int64(c.Config.RaftElectionTimeout) {
			common.Debugf("[Candidate] state duration [%d], detectDownTimes [%d]", duration, *detectDownTimes)
			*detectDownTimes++
			if *detectDownTimes > 3 {
				*detectDownTimes = 0
				c.handleClusterDowntime()
			} else {
				c.SelfNode.LastStateUpdateTime = time.Now()
			}
		}
	}
}

func (c *Cluster) transferLeaderShip() {

	c.transferMtx.Lock()
	defer c.transferMtx.Unlock()

	leaderAddr, leaderId := c.Raft.LeaderWithID()
	stats := c.Raft.Stats()
	term, _ := strconv.Atoi(stats[TermKey])

	// First of all, we need to confirm which followers are led by the leader and whether the follower status is normal
	future := c.Raft.VerifyLeader()
	if future.Error() != nil {
		// If the current node is the leader node before, the cluster is down or the
		// network is partitioned, and only the current node remains in the cluster
		if leaderId == "" {
			common.Errorf("cluster is down or the network is partitioned, and only the current node remains in the cluster")
			go c.handleClusterDowntime()
		}
		common.Errorf("failed to transfer leadership: %s", future.Error())
		return
	}

	// The first thing you need to do is set the cluster state to StandBy
	// In this case, the cluster only receives read requests and cannot process write requests
	c.SetState(Transitioning)

	jsonStr, _ := json.Marshal(stats)
	common.Debugf("[leader] transfer leadership to %s, raft stats : %s", leaderId, jsonStr)

	c.UpdateLeader(uint64(term), string(leaderId), string(leaderAddr))

	raftConf := c.Raft.GetConfiguration().Configuration()
	nodes := c.getNodesByRaftServers(raftConf.Servers)

	c.Leader.buildCluster(nodes)
}

func (c *Cluster) joinToCluster() {
	connectedNodes := c.Manager.GetConnectedNodes(&c.ClusterExpectedNodes)
	for _, node := range connectedNodes {
		rpcClient := c.Manager.GetInterRpcClient(node.Id)
		request := pb.JoinClusterRequest{
			NodeId:                c.SelfNode.Id,
			NodeAddr:              c.SelfNode.Addr,
			AutoJoinClusterEnable: c.SelfNode.AutoJoinClusterEnable,
			IsCandidate:           c.SelfNode.IsCandidate,
		}
		_, err := (*rpcClient).JoinCluster(context.Background(), &request)
		if err != nil {
			common.Errorf("failed to join cluster from remote node, id: %s, addr: %s, %s", node.Id, node.Addr, err.Error())
		}
	}
}
