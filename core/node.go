package core

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	network "github.com/linkypi/hiraeth.registry/core/network"
	raftx "github.com/linkypi/hiraeth.registry/core/raft"
	"github.com/linkypi/hiraeth.registry/core/service"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/sirupsen/logrus"
	"github.com/sourcegraph/conc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
	"time"
)

type Node struct {
	log     *logrus.Logger
	Config  config.NodeConfig
	network *network.NetworkManager
	raft    *raft.Raft

	socket         net.Listener
	grpcServer     *grpc.Server
	shutDownCh     chan struct{}
	notifyLeaderCh chan bool
}

func NewNode(serverConfig config.NodeConfig, logger *logrus.Logger) *Node {
	return &Node{Config: serverConfig, log: logger}
}

func (n *Node) Start() {

	address := raft.ServerAddress(n.Config.SelfNode.Addr)
	n.network = network.NewNetworkManager(address)

	// since grpc will enter a loop after starting
	// we need to use a channel to notify grpcServer
	// has assigned if it has been started
	rpcStartCh := make(chan struct{})
	go n.startGRPCServer(rpcStartCh)
	select {
	case <-rpcStartCh:
		break
	case <-n.shutDownCh:
		n.Shutdown()
		return
	}

	if !n.Config.StandAlone {
		n.startCluster()
	}
}

func (n *Node) startCluster() {
	// try connecting to other candidate nodes
	go n.connectOtherCandidateNodes()

	// wait for the quorum nodes to be connected
	for len(n.network.Connections) < n.Config.ClusterQuorumCount {
		time.Sleep(200 * time.Millisecond)
	}

	//  start the raft server
	n.startRaftNode()

	go n.monitorLeaderChanged()

	select {
	case <-n.shutDownCh:
		n.Shutdown()
	}
}

func (n *Node) startRaftNode() {
	raftNode := raftx.RaftNode{}
	raftNode.SetNetWorkManager(n.network)

	selfNode := n.Config.SelfNode
	propFsm := &raftx.PropFsm{}

	// The nodes that come in here should be the nodes that
	// have been connected and satisfy the election quorum number
	// not the nodes in the cluster.server.addr configuration
	connectedNodes := n.network.GetConnectedNodes(n.Config.ClusterServers)
	connectedNodes = append(connectedNodes, n.Config.SelfNode)
	var peers = make([]raft.Server, 0, len(connectedNodes))
	for _, node := range connectedNodes {
		server := raft.Server{
			ID:      raft.ServerID(node.Id),
			Address: raft.ServerAddress(node.Addr),
		}
		peers = append(peers, server)
	}

	raftFsm, err := raftNode.Start(selfNode.Id, n.Config.DataDir, peers, n.notifyLeaderCh, propFsm)
	if err != nil {
		n.log.Errorf("failed to start raft node: %v", err)
		close(n.shutDownCh)
		time.Sleep(time.Second)
		return
	}
	n.raft = raftFsm
	n.log.Infof("raft node started.")
}

// monitor leader changes
func (n *Node) monitorLeaderChanged() {
	isDown := false
	for {
		select {
		case leader := <-n.raft.LeaderCh():
			n.log.Info("leader changed to %s", leader)
			// do something
		case changed := <-n.notifyLeaderCh:
			n.log.Info("leader has changed", changed)
		case <-n.shutDownCh:
			isDown = true
			break
		}
		if isDown {
			break
		}
	}
}

func (n *Node) startGRPCServer(init chan struct{}) {
	sock, err := net.Listen("tcp", n.Config.SelfNode.Addr)
	if err != nil {
		n.log.Errorf("failed to listen: %v", err)
		close(n.shutDownCh)
		return
	}
	n.socket = sock

	var kaep = keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	var kasp = keepalive.ServerParameters{
		// If a client is idle for 15 seconds, send a GOAWAY
		MaxConnectionIdle: 15 * time.Second,
		// If any connection is alive for more than 30 seconds, send a GOAWAY
		MaxConnectionAge: 30 * time.Second,
		// Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		MaxConnectionAgeGrace: 5 * time.Second,
		// Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Time: time.Duration(n.Config.ClusterHeartbeatInterval) * time.Second,
		// Wait 1 second for the ping ack before assuming the connection is dead
		Timeout: 1 * time.Second,
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	service.RegisterRpcService(grpcServer, n.Config, n.network)

	n.grpcServer = grpcServer
	reflection.Register(grpcServer)
	init <- struct{}{}
	if err := n.grpcServer.Serve(n.socket); err != nil {
		n.log.Errorf("grpc server failed to serve: %v", err)
		close(n.shutDownCh)
	}
}

func (n *Node) Shutdown() {
	n.log.Info("shutting down the server.")
	n.network.Close()
	n.grpcServer.GracefulStop()
	n.socket.Close()
	close(n.shutDownCh)
	time.Sleep(time.Second)
}

func (n *Node) connectOtherCandidateNodes() {

	var wg conc.WaitGroup
	// we can't know if the other nodes in the cluster is candidate node,
	// so we can't get the node information from Config.OtherCandidateServers(exclude self node)
	// we can only use Config.ClusterServers to connect to the cluster nodes and then
	// use GetNodeInfo to get information about each node in the cluster
	for _, node := range n.Config.GetOtherNodes() {
		remote := node
		wg.Go(func() {
			n.connectToNode(remote)
		})
	}
	wg.Wait()
	n.log.Info("all the other candidate nodes are connected.")
}

func (n *Node) connectToNode(remoteNode config.NodeInfo) {
	retries := 1
	var kacp = keepalive.ClientParameters{
		// send pings every ClusterHeartbeatInterval seconds if there is no activity
		Time: time.Duration(n.Config.ClusterHeartbeatInterval) * time.Second,
		// wait 1 second for ping ack before considering the connection dead
		Timeout: time.Second,
		// send pings even without active streams
		PermitWithoutStream: true,
	}

	for {
		conn, err := grpc.Dial(remoteNode.Addr, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
		if err != nil {
			n.log.Errorf("failed to dial server: %s, retry time: %d. %v", remoteNode.Addr, retries, err)
			retries++
			time.Sleep(time.Second)
			continue
		}

		conn.Connect()
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()
		conn.WaitForStateChange(ctx, connectivity.Connecting)

		if conn.GetState() == connectivity.Ready {
			n.log.Infof("connected to node: %s:%s", remoteNode.Id, remoteNode.Addr)

			// record remote connection
			clusterServiceClient := pb.NewClusterServiceClient(conn)
			raftTransportClient := pb.NewRaftTransportClient(conn)
			n.network.AddConn(raft.ServerID(remoteNode.Id), conn, clusterServiceClient, raftTransportClient)

			n.getNodeInfo(remoteNode)

			break
		}

		time.Sleep(3 * time.Second)
		n.log.Infof("waiting for node: %s:%s to be ready", remoteNode.Id, remoteNode.Addr)
	}
}

func (n *Node) obtainOtherNodeInfo() {
	var wg conc.WaitGroup
	for _, node := range n.Config.GetOtherNodes() {
		wg.Go(func() {
			n.getNodeInfo(node)
		})
	}
	// handle the problem of cluster configuration mismatch
	// if the cluster configuration does not match, it will exit directly
	defer func() {
		if r := recover(); r != nil {
			close(n.shutDownCh)
		} else {
			n.log.Info("obtain all the other node info success.")
		}
	}()

	wg.WaitAndRecover()
}

// exchange information between the two nodes in preparation for the creation of a cluster
func (n *Node) getNodeInfo(remoteNode config.NodeInfo) {

	// handle the problem of cluster configuration mismatch when updating remote node
	// if the cluster configuration does not match, it will exit directly
	defer func() {
		if r := recover(); r != nil {
			close(n.shutDownCh)
		}
	}()

	retries := 0
	for {
		currentNode := n.Config.SelfNode
		request := pb.NodeInfoRequest{
			NodeId:       currentNode.Id,
			NodeIp:       currentNode.Ip,
			InternalPort: uint64(currentNode.InternalPort),
			IsCandidate:  currentNode.IsCandidate,
		}
		client := n.network.GetInternalClient(raft.ServerID(remoteNode.Id))
		response, err := client.GetNodeInfo(context.Background(), &request)
		if err != nil {
			connected := n.network.IsConnected(raft.ServerID(remoteNode.Id))
			if !connected {
				n.log.Errorf("remote node [%s][%s] is disconnected.", remoteNode.Id, remoteNode.Addr)
				n.connectToNode(remoteNode)
				continue
			}
			n.log.Errorf("failed to get node info from %s - %s, retry time: %d, . %v", remoteNode.Id, remoteNode.Addr, retries, err)
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
		n.Config.UpdateRemoteNode(remoteNode, true)
		break
	}
}
