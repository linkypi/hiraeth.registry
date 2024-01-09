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
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
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

	n.startGRPCServer()

	n.network = network.NewNetworkManager(raft.ServerAddress(n.Config.SelfNode.Addr))
	// Try connecting to other candidate nodes
	n.waitAllTheOtherCandidateConnected()

	// obtain information about other nodes to prepare for bootstrap the cluster
	n.obtainOtherNodeInfo()

	//  start the raft server
	go n.startRaftNode()

	go monitorLeaderChanged(n.raft.LeaderCh(), n.notifyLeaderCh, n.log)

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

	var peers = make([]raft.Server, 3)
	for _, node := range n.Config.ClusterServers {
		server := raft.Server{
			ID:      raft.ServerID(node.Id),
			Address: raft.ServerAddress(node.Addr),
		}
		peers = append(peers, server)
	}

	raftFsm, err := raftNode.StartRaftNode(selfNode.Id, n.Config.LogDir,
		peers, n.grpcServer, n.notifyLeaderCh, propFsm)
	if err != nil {
		n.log.Errorf("failed to start raft node: %v", err)
		n.shutDownCh <- struct{}{}
		return
	}
	n.raft = raftFsm
	n.log.Infof("raft node started.")
}

// monitor leader changes
func monitorLeaderChanged(leaderCh <-chan bool, notifyCh chan bool, log *logrus.Logger) {
	for {
		select {
		case leader := <-leaderCh:
			log.Info("leader changed to %s", leader)
			// do something
		case changed := <-notifyCh:
			log.Info("leader has changed", changed)
		}

	}

}

func (n *Node) startGRPCServer() {
	sock, err := net.Listen("tcp", n.Config.SelfNode.Addr)
	if err != nil {
		n.log.Errorf("failed to listen: %v", err)
		panic(err)
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
	service.RegisterRpcService(grpcServer, n.Config)

	n.grpcServer = grpcServer
	reflection.Register(grpcServer)
	if err := grpcServer.Serve(sock); err != nil {
		n.log.Errorf("grpc server failed to serve: %v", err)
		grpcServer.GracefulStop()
		sock.Close()
		os.Exit(1)
	}
}

func (n *Node) Shutdown() {
	n.log.Info("shutting down the server.")
	n.grpcServer.GracefulStop()
	n.socket.Close()
	close(n.shutDownCh)
}

func (n *Node) waitAllTheOtherCandidateConnected() {

	var wg conc.WaitGroup
	for _, node := range n.Config.OtherCandidateNodes {
		wg.Go(func() {
			n.connectToNode(node)
		})
	}
	wg.Wait()
}

func (n *Node) connectToNode(nodeInfo config.NodeInfo) {
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
		conn, err := grpc.Dial(nodeInfo.Addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithKeepaliveParams(kacp))
		if err != nil {
			n.log.Errorf("failed to dial server: %s, retry time: %d, . %v", nodeInfo.Addr, retries, err)
			retries++
			time.Sleep(500 * time.Millisecond)
			continue
		}

		client := pb.NewInternalServiceClient(conn)
		// Record remote connections
		n.network.AddConn(raft.ServerID(nodeInfo.Id), conn, client)
		break
	}
}

func (n *Node) obtainOtherNodeInfo() {
	var wg conc.WaitGroup
	for _, node := range n.Config.OtherCandidateNodes {
		wg.Go(func() {
			n.getNodeInfo(node)
		})
	}
	// handle the problem of cluster configuration mismatch
	// if the cluster configuration does not match, it will exit directly
	defer func() {
		if r := recover(); r != nil {
			n.shutDownCh <- struct{}{}
		}
	}()

	wg.WaitAndRecover()
}

// exchange information between the two nodes in preparation for the creation of a cluster
func (n *Node) getNodeInfo(nodeInfo config.NodeInfo) {
	retries := 0
	for {
		currentNode := n.Config.SelfNode
		request := pb.NodeInfoRequest{
			NodeId:       currentNode.Id,
			NodeIp:       currentNode.Ip,
			InternalPort: uint64(currentNode.InternalPort),
			IsCandidate:  currentNode.IsCandidate,
		}
		client := n.network.GetInternalClient(raft.ServerID(nodeInfo.Id))
		response, err := client.GetNodeInfo(context.Background(), &request)
		if err != nil {
			n.log.Errorf("failed to get node info from %s - %s, retry time: %d, . %v", nodeInfo.Id, nodeInfo.Addr, retries, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		remoteNode := config.NodeInfo{
			Id:          response.NodeId,
			Ip:          response.NodeIp,
			Addr:        response.NodeIp + ":" + strconv.Itoa(int(response.InternalPort)),
			IsCandidate: response.IsCandidate,
		}

		// update cluster node config
		n.Config.UpdateRemoteNode(remoteNode, true)
	}
}
