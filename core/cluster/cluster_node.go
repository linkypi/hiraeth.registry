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
	for len(c.net.Connections) < c.Config.ClusterQuorumCount {
		time.Sleep(200 * time.Millisecond)
	}

	//  start the raft server
	c.startRaftNode(dataDir)

	go c.monitorLeaderChanged()
}

func (c *Cluster) startRaftNode(dataDir string) {
	raftNode := hraft.RaftNode{}
	raftNode.SetNetWorkManager(c.net)

	selfNode := c.SelfNode
	propFsm := &hraft.PropFsm{}

	// The nodes that come in here should be the nodes that
	// have been connected and satisfy the election quorum number
	// not the nodes in the cluster.server.addr configuration
	connectedNodes := c.net.GetConnectedNodes(c.Config.ClusterServers)
	connectedNodes = append(connectedNodes, *c.SelfNode)
	var peers = make([]raft.Server, 0, len(connectedNodes))
	for _, node := range connectedNodes {
		server := raft.Server{
			ID:      raft.ServerID(node.Id),
			Address: raft.ServerAddress(node.Addr),
		}
		peers = append(peers, server)
	}

	raftFsm, err := raftNode.Start(selfNode.Id, dataDir, peers, *c.Config, c.notifyLeaderCh, propFsm)
	if err != nil {
		c.Log.Errorf("failed to start raft node: %v", err)
		c.Shutdown()
		return
	}
	c.Raft = raftFsm
	c.Log.Infof("raft node started.")
}

// monitor leader changes
func (c *Cluster) monitorLeaderChanged() {
	isDown := false
	for {
		select {
		case leader := <-c.Raft.LeaderCh():
			c.Log.Infof("leader changed to %v", leader)
			// do something
		case changed := <-c.notifyLeaderCh:
			c.Log.Info("leader has changed", changed)
		case <-c.ShutDownCh:
			isDown = true
			break
		}
		if isDown {
			break
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

	c.net.Close()
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
			c.Log.Infof("connected to node: %s:%s", remoteNode.Id, remoteNode.Addr)

			// record remote connection
			clusterServiceClient := pb.NewClusterServiceClient(conn)
			raftTransportClient := pb.NewRaftTransportClient(conn)
			c.net.AddConn(remoteNode.Id, conn, clusterServiceClient, raftTransportClient)

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
		currentNode := c.SelfNode
		request := pb.NodeInfoRequest{
			NodeId:       currentNode.Id,
			NodeIp:       currentNode.Ip,
			InternalPort: uint64(currentNode.InternalPort),
			IsCandidate:  currentNode.IsCandidate,
		}
		client := c.net.GetInternalClient(remoteNode.Id)
		response, err := client.GetNodeInfo(context.Background(), &request)
		if err != nil {
			connected := c.net.IsConnected(remoteNode.Id)
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
			Id:           response.NodeId,
			Ip:           response.NodeIp,
			Addr:         response.NodeIp + ":" + strconv.Itoa(int(response.InternalPort)),
			InternalPort: int(response.InternalPort),
			IsCandidate:  response.IsCandidate,
		}

		// update cluster node config
		_ = c.UpdateRemoteNode(remoteNode, *c.SelfNode, true)
		break
	}
}
