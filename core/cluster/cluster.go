package cluster

import (
	"context"
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/slot"
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

const (
	MetaDataFileName = "/metadata.json"
	TermKey          = "term"
)

func NewCluster(conf *config.Config, selfNode *config.NodeInfo,
	net *network.NetworkManager, shutDownCh chan struct{}, log *logrus.Logger) *Cluster {

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
			notifyLeaderCh:       make(chan bool, 10),
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

	go c.monitoringLeadershipTransfer()

	go c.CheckConnClosed(c.ShutDownCh, func(id string) {
		nodeInfo := c.ClusterActualNodes[id]
		c.Log.Infof("node %s is disconnected, re-establish the connection: %s", nodeInfo.Id, nodeInfo.Addr)
		c.ConnectToNode(*nodeInfo)
	})

	if c.SelfNode.AutoJoinClusterEnable && c.joinCluster {
		go func() { c.joinToCluster() }()
	}
}

// 区分当前健康集群的节点列表以及配置中期望的集群节点列表
func (c *Cluster) checkClusterState() {

}

func (c *Cluster) monitoringLeadershipTransfer() {
	for {
		select {
		case _ = <-c.Raft.LeaderCh():
			go c.transferLeaderShip()

		// Multiple guarantees to obtain leadership transfer notices
		case _ = <-c.notifyLeaderCh:
			go c.transferLeaderShip()

		case <-c.ShutDownCh:
			return
		}
	}
}

func (c *Cluster) transferLeaderShip() {

	c.transferMtx.Lock()
	defer c.transferMtx.Unlock()

	oldLeader := c.BaseCluster.Leader

	leaderAddr, leaderId := c.Raft.LeaderWithID()
	stats := c.Raft.Stats()
	term, _ := strconv.Atoi(stats[TermKey])

	//  If the leader is not changed, it will not be executed
	if oldLeader != nil && oldLeader.Id == string(leaderId) && oldLeader.Term == term {
		// 很有可能是新节点加入集群,此时就需要读取metadata.json中的数据来判断前后的follower数量是否一致
		// 若follower数量不一致则说明是新节点加入,需要重新调整数据分片
		return
	}

	// First of all, we need to confirm which followers are led by the leader and whether the follower status is normal
	future := c.Raft.VerifyLeader()
	if future.Error() != nil {
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
	c.Leader.buildCluster(nodes)
}

func (c *Cluster) joinToCluster() {
	connectedNodes := c.NetworkManager.GetConnectedNodes(c.ClusterExpectedNodes)
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
