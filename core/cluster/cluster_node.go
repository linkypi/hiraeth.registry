package cluster

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	hraft "github.com/linkypi/hiraeth.registry/core/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/sourcegraph/conc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"strconv"
	"time"
)

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
		c.connectToNode(*nodeInfo)
	})
}

func (c *Cluster) joinCluster() {
	if !c.SelfNode.AutoJoinClusterEnable {
		c.Log.Debugf("disabled auto join cluster, abandon sending join cluster request.")
		return
	}

	connectedNodes := c.NetworkManager.GetConnectedNodes(c.ClusterServers)
	for _, node := range connectedNodes {
		client := c.NetworkManager.GetInternalClient(node.Id)
		request := pb.JoinClusterRequest{
			NodeId:                c.SelfNode.Id,
			NodeAddr:              c.SelfNode.Addr,
			AutoJoinClusterEnable: c.SelfNode.AutoJoinClusterEnable,
			IsCandidate:           c.SelfNode.IsCandidate,
		}
		_, err := client.JoinCluster(context.Background(), &request)
		if err != nil {
			c.Log.Errorf("failed to join cluster from remote node, id: %s, addr: %s, %s", node.Id, node.Addr, err.Error())
		}
	}
}

func (c *Cluster) startRaftNode(dataDir string) {
	raftNode := hraft.RaftNode{}
	raftNode.SetNetWorkManager(c.NetworkManager)

	selfNode := c.SelfNode
	propFsm := &hraft.PropFsm{}

	// The nodes that come in here should be the nodes that
	// have been connected and satisfy the election quorum number
	// not the nodes in the cluster.server.addr configuration
	connectedNodes := c.GetConnectedNodes(c.Config.ClusterServers)
	connectedNodes = append(connectedNodes, *c.SelfNode)
	var peers = make([]raft.Server, 0, len(connectedNodes))
	for _, node := range connectedNodes {
		suffrage := raft.Voter
		if !node.IsCandidate {
			suffrage = raft.Nonvoter
		}
		server := raft.Server{
			// Suffrage specify whether it is a candidate node or not, it can participate in the leader election
			Suffrage: suffrage,
			ID:       raft.ServerID(node.Id),
			Address:  raft.ServerAddress(node.Addr),
		}
		peers = append(peers, server)
	}

	raftFsm, err := raftNode.Start(selfNode.Id, dataDir, peers, *c.Config, c.notifyLeaderCh, propFsm)
	if err != nil {
		c.Log.Errorf("failed to start raft node: %v", err.Error())
		c.Shutdown()
		return
	}
	c.Raft = raftFsm
	c.Log.Infof("raft node started.")
}

func (c *Cluster) monitoringLeadershipTransfer() {
	isDown := false
	for {
		select {
		case _ = <-c.Raft.LeaderCh():

			addr, serverID := c.Raft.LeaderWithID()
			c.Log.Infof("leader changed to %s:%s", serverID, addr)
			c.UpdateLeader(string(serverID), string(addr))
			c.notifyLeaderShipTransfer()

		case _ = <-c.notifyLeaderCh:
			addr, serverID := c.Raft.LeaderWithID()
			if serverID == "" || addr == "" {
				c.State = StandBy
			} else {
				c.UpdateLeader(string(serverID), string(addr))
				c.notifyLeaderShipTransfer()
			}
			c.Log.Infof("leadership has changed from %s to %s", c.Leader.Addr, addr)

		case <-c.ShutDownCh:
			isDown = true
			break
		}
		if isDown {
			break
		}
	}
}

func (c *Cluster) notifyLeaderShipTransfer() {
	request := pb.TransferRequest{
		NodeId:   c.Leader.Id,
		NodeAddr: c.Leader.Addr,
	}
	for _, node := range c.GetConnectedNodes(c.ClusterServers) {
		client := c.NetworkManager.GetInternalClient(node.Id)
		_, err := client.TransferLeadership(context.Background(), &request)
		if err != nil {
			c.Log.Errorf("failed to notify %s:%s leader transferred, %v", node.Id, node.Addr, err)
		}
	}
}

func (c *Cluster) connectOtherCandidateNodes() {
	otherNodes := c.GetOtherNodes(c.SelfNode.Id)
	if len(otherNodes) == 0 {
		// this rarely happens because the list of servers in cluster mode
		// has already been determined when the node starts
		c.Log.Error("failed to get other candidate nodes")
		c.Shutdown()
		return
	}

	// we can't know if the other nodes in the cluster is candidate node,
	// so we can't get the node information from Config.OtherCandidateServers(exclude self node)
	// we can only use Config.ClusterServers to connect to the cluster nodes and then
	// use GetNodeInfo to get information about each node in the cluster
	var wg conc.WaitGroup
	for _, node := range c.GetOtherNodes(c.SelfNode.Id) {
		remote := node
		wg.Go(func() {
			c.connectToNode(remote)
		})
	}
	wg.Wait()
	c.Log.Info("all the other candidate nodes are connected.")
}

func (c *Cluster) Shutdown() {
	if c.Raft != nil {
		future := c.Raft.Shutdown()
		err := future.Error()
		if err != nil {
			c.Log.Errorf("raft shutdown failed: %v", err)
		}
	}

	c.NetworkManager.Close()
	close(c.ShutDownCh)
	time.Sleep(time.Second)
}

func (c *Cluster) connectToNode(remoteNode config.NodeInfo) {
	retries := 1
	var kacp = keepalive.ClientParameters{
		// send pings every ClusterHeartbeatInterval seconds if there is no activity
		Time: time.Duration(c.Config.ClusterHeartbeatInterval) * time.Second,
		// wait 1 second for ping ack before considering the connection dead
		Timeout: time.Second,
		// send pings even without active streams
		PermitWithoutStream: true,
	}

	for {
		select {
		case <-c.ShutDownCh:
			return
		default:
		}
		conn, err := grpc.Dial(remoteNode.Addr, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
		if err != nil {
			c.Log.Errorf("failed to dial server: %s, retry time: %d. %v", remoteNode.Addr, retries, err)
			retries++
			time.Sleep(time.Second)
			continue
		}

		conn.Connect()
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()
		conn.WaitForStateChange(ctx, connectivity.Connecting)

		if conn.GetState() == connectivity.Ready {
			c.Log.Infof("connected to node: %s, %s", remoteNode.Id, remoteNode.Addr)

			// record remote connection
			clusterServiceClient := pb.NewClusterServiceClient(conn)
			raftTransportClient := pb.NewRaftTransportClient(conn)
			c.AddConn(remoteNode.Id, conn, clusterServiceClient, raftTransportClient)
			c.Log.Infof("The connection has saved %s, %s.", remoteNode.Id, remoteNode.Addr)

			c.getNodeInfo(remoteNode)
			break
		}

		time.Sleep(3 * time.Second)
		c.Log.Debugf("waiting for node: %s:%s to be ready", remoteNode.Id, remoteNode.Addr)
	}
}

// exchange information between the two nodes in preparation for the creation of a myCluster
func (c *Cluster) getNodeInfo(remoteNode config.NodeInfo) {

	// handle the problem of cluster configuration mismatch when updating remote node
	// if the cluster configuration does not match, it will exit directly
	defer func() {
		if r := recover(); r != nil {
			c.Shutdown()
		}
	}()

	retries := 0
	for {
		select {
		case <-c.ShutDownCh:
			return
		default:
		}
		currentNode := c.SelfNode
		request := pb.NodeInfoRequest{
			NodeId:                currentNode.Id,
			NodeIp:                currentNode.Ip,
			InternalPort:          uint64(currentNode.InternalPort),
			IsCandidate:           currentNode.IsCandidate,
			AutoJoinClusterEnable: currentNode.AutoJoinClusterEnable,
		}
		client := c.GetInternalClient(remoteNode.Id)
		response, err := client.GetNodeInfo(context.Background(), &request)
		if err != nil {
			connected := c.IsConnected(remoteNode.Id)
			if !connected {
				c.Log.Errorf("remote node [%s][%s] is disconnected.", remoteNode.Id, remoteNode.Addr)
				c.connectToNode(remoteNode)
				continue
			}
			c.Log.Errorf("failed to get node info from %s - %s, retry time: %d, . %v", remoteNode.Id, remoteNode.Addr, retries, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		remoteNode := config.NodeInfo{
			Id:                    response.NodeId,
			Ip:                    response.NodeIp,
			Addr:                  response.NodeIp + ":" + strconv.Itoa(int(response.InternalPort)),
			InternalPort:          int(response.InternalPort),
			IsCandidate:           response.IsCandidate,
			AutoJoinClusterEnable: response.AutoJoinClusterEnable,
		}

		// update cluster node config
		_ = c.UpdateRemoteNode(remoteNode, *c.SelfNode, true)
		break
	}
}
