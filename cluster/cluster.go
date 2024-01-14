package cluster

import (
	"context"
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/network"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/linkypi/hiraeth.registry/slot"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"sync/atomic"
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

func NewCluster(conf *config.Config, selfNode *config.NodeInfo,
	net *network.Manager, shutDownCh chan struct{}, log *logrus.Logger) *Cluster {

	slotManager := slot.NewManager(log, selfNode.Id, conf.NodeConfig.DataDir, conf.ClusterConfig.NumberOfReplicas)
	cluster := Cluster{
		BaseCluster: &BaseCluster{
			Log:                  log,
			joinCluster:          conf.JoinCluster,
			State:                Initializing,
			Config:               &conf.ClusterConfig,
			nodeConfig:           &conf.NodeConfig,
			ShutDownCh:           shutDownCh,
			SelfNode:             selfNode,
			notifyCh:             make(chan bool, 10),
			ClusterExpectedNodes: conf.ClusterConfig.ClusterServers,
			ClusterActualNodes:   conf.ClusterConfig.ClusterServers,
			slotManager:          slotManager,
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
		nodeInfo := c.ClusterActualNodes[id]
		c.Log.Infof("node %s is disconnected, re-establish the connection: %s", nodeInfo.Id, nodeInfo.Addr)
		c.ConnectToNode(*nodeInfo)
	})

	if c.SelfNode.AutoJoinClusterEnable && c.joinCluster {
		go func() { c.joinToCluster() }()
	}
}

func (c *Cluster) handleClusterDowntime() {

	if c.State == Down {
		return
	}

	c.Log.Errorf("cluster is down or the network partitioned")
	c.SetState(Down)
}

func (c *Cluster) monitoringClusterState() {
	detectDownTimes := 1
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
	c.Log.Debugf(" node state [%s]", state)

	if state != string(c.SelfNode.State) {
		c.SelfNode.State = config.NodeState(state)
		c.SelfNode.LastStateUpdateTime = time.Now()
	} else {
		// If the current node has become a candidate node and the duration
		// has exceeded one election cycles, detect three times. If the conditions are met, the cluster is down
		duration := time.Now().Sub(c.SelfNode.LastStateUpdateTime).Milliseconds()
		if c.SelfNode.State == config.Candidate && duration > int64(c.Config.RaftElectionTimeout) {
			c.Log.Debugf("[Candidate] state duration [%d], detectDownTimes [%d]", duration, *detectDownTimes)
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
			c.Log.Errorf("cluster is down or the network is partitioned, and only the current node remains in the cluster")
			go c.handleClusterDowntime()
		}
		c.Log.Errorf("failed to transfer leadership: %s", future.Error())
		return
	}

	// The first thing you need to do is set the cluster state to StandBy
	// In this case, the cluster only receives read requests and cannot process write requests
	c.SetState(Transitioning)

	jsonStr, _ := json.Marshal(stats)
	c.Log.Debugf("[leader] transfer leadership to %s, raft stats : %s", leaderId, jsonStr)

	c.UpdateLeader(term, string(leaderId), string(leaderAddr))

	raftConf := c.Raft.GetConfiguration().Configuration()
	nodes := c.getNodesByRaftServers(raftConf.Servers)

	atomic.AddUint64(&c.clusterId, 1)
	c.Leader.buildCluster(nodes)
}

func (c *Cluster) joinToCluster() {
	connectedNodes := c.Manager.GetConnectedNodes(c.ClusterExpectedNodes)
	for _, node := range connectedNodes {
		rpcClient := c.Manager.GetInterRpcClient(node.Id)
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
