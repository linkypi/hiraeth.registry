package core

import (
	"github.com/linkypi/hiraeth.registry/config"
	cluster "github.com/linkypi/hiraeth.registry/core/cluster"
	"github.com/linkypi/hiraeth.registry/core/cluster/rpc"
	network "github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/service"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"time"
)

type Node struct {
	log      *logrus.Logger
	selfNode *config.NodeInfo
	//myCluster  *cluster.Cluster
	rpcService *rpc.ClusterRpcService
	Config     config.NodeConfig
	Network    *network.NetworkManager
	socket     net.Listener
	grpcServer *grpc.Server
	shutDownCh chan struct{}
}

func NewNode(config config.Config, logger *logrus.Logger) *Node {
	return &Node{
		selfNode:   config.NodeConfig.SelfNode,
		Config:     config.NodeConfig,
		rpcService: rpc.NewCRpcService(),
		log:        logger}
}

func (n *Node) Start(clusterConfig *config.ClusterConfig) {

	n.Network = network.NewNetworkManager(n.selfNode.Addr, n.log)

	// since grpc will enter a loop after starting
	// we need to use a channel to notify grpcServer
	// has assigned if it has been started
	rpcStartCh := make(chan struct{})
	go n.startGRPCServer(rpcStartCh)
	select {
	// wait for grpcServer to start, because grpc server
	// needs to be used to register the grpc service
	case <-rpcStartCh:
		break
	case <-n.shutDownCh:
		n.Shutdown()
		return
	}

	if clusterConfig.StartupMode == config.Cluster {
		myCluster := cluster.NewCluster(clusterConfig, n.selfNode, n.Network, n.shutDownCh, n.log)
		n.rpcService.SetCluster(myCluster)
		go myCluster.Start(n.Config.DataDir)
	}

	select {
	case <-n.shutDownCh:
		n.Shutdown()
	}
}

func (n *Node) startGRPCServer(grpcAssignCh chan struct{}) {
	sock, err := net.Listen("tcp", n.selfNode.Addr)
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
		Time: time.Duration(n.Config.HeartbeatInterval) * time.Second,
		// Wait 1 second for the ping ack before assuming the connection is dead
		Timeout: 1 * time.Second,
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	service.RegisterRpcService(grpcServer, n.rpcService, n.Network)

	n.grpcServer = grpcServer
	reflection.Register(grpcServer)
	// notify if grpcServer has been created
	grpcAssignCh <- struct{}{}

	// start grpc server，enter an infinite loop after the startup is complete
	if err := n.grpcServer.Serve(n.socket); err != nil {
		n.log.Errorf("grpc server failed to serve: %v", err)
		close(n.shutDownCh)
	}
}

func (n *Node) Shutdown() {
	n.log.Info("shutting down the server.")

	n.grpcServer.GracefulStop()
	n.socket.Close()

	// wait for the server to shut down
	time.Sleep(time.Second)
	n.log.Info("server is down gracefully.")
	os.Exit(0)
}
