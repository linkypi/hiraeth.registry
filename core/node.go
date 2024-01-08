package core

import (
	"context"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	raftx "github.com/linkypi/hiraeth.registry/core/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/linkypi/hiraeth.registry/service"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	log          *logrus.Logger
	serverConfig config.ServerConfig
	network      *NetworkManager
	raft         *raft.Raft

	socket     net.Listener
	grpcServer *grpc.Server
	shutDownCh chan struct{}
}

func NewNode(serverConfig config.ServerConfig, logger *logrus.Logger) *Node {
	return &Node{serverConfig: serverConfig, log: logger}
}

func (n *Node) Start() {

	n.startGRPCServer()

	n.network = NewNetworkManager(raft.ServerAddress(n.serverConfig.CurrentNode.Addr))
	// Try connecting to other candidate nodes
	var connWg sync.WaitGroup
	go n.connectToOtherCandidates(n.serverConfig, &connWg)
	connWg.Wait()

	//  start the raft server
	go n.startRaftNode()

	select {
	case <-n.shutDownCh:
		n.Shutdown()
	}
}

func (n *Node) startRaftNode() {
	raftNode := raftx.RaftNode{}
	raftNode.SetNetWorkManager(n.network)

	currentNode := n.serverConfig.CurrentNode
	propFsm := &raftx.PropFsm{}
	raftFsm, err := raftNode.StartRaftNode(strconv.Itoa(currentNode.Id),
		currentNode.Addr, n.serverConfig.LogDir, n.grpcServer, propFsm)
	if err != nil {
		n.log.Errorf("failed to start raft node: %v", err)
		n.shutDownCh <- struct{}{}
		return
	}
	n.raft = raftFsm
	n.log.Infof("raft node started.")
}

func (n *Node) startGRPCServer() {
	sock, err := net.Listen("tcp", n.serverConfig.CurrentNode.Addr)
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
		Time: time.Duration(n.serverConfig.ClusterHeartbeatInterval) * time.Second,
		// Wait 1 second for the ping ack before assuming the connection is dead
		Timeout: 1 * time.Second,
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	pb.RegisterInternalServiceServer(grpcServer, &service.InternalServiceServer{ServerConfig: n.serverConfig})

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

func (n *Node) connectToOtherCandidates(config config.ServerConfig, connWg *sync.WaitGroup) {
	for _, node := range config.OtherCandidateNodes {
		connWg.Add(1)
		node := node
		go func() {
			retries := 1
			for {
				var kacp = keepalive.ClientParameters{
					// send pings every ClusterHeartbeatInterval seconds if there is no activity
					Time: time.Duration(n.serverConfig.ClusterHeartbeatInterval) * time.Second,
					// wait 1 second for ping ack before considering the connection dead
					Timeout: time.Second,
					// send pings even without active streams
					PermitWithoutStream: true,
				}
				conn, err := grpc.Dial(node.Addr, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
				if err != nil {
					n.log.Errorf("failed to dial server: %s, retry again: %d, . %v", node.Addr, retries, err)
					retries++
					continue
				}

				client := pb.NewInternalServiceClient(conn)

				// exchange information between the two nodes in preparation for the creation of a cluster
				currentNode := config.CurrentNode
				request := pb.NodeInfoRequest{
					NodeId:       uint32(currentNode.Id),
					NodeIp:       currentNode.Ip,
					InternalPort: uint32(currentNode.InternalPort),
				}
				nodeInfo, err := client.GetNodeInfo(context.Background(), &request)

				// update cluster node config
				config.UpdateNode(int(nodeInfo.NodeId), nodeInfo.NodeIp, int(nodeInfo.InternalPort))

				// Record remote connections
				n.network.AddConn(raft.ServerID(strconv.Itoa(int(nodeInfo.NodeId))), conn, client)

				connWg.Done()
				break
			}
		}()

	}
}
