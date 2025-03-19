package main

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/api/handler"
	"github.com/linkypi/hiraeth.registry/server/api/http"
	"github.com/linkypi/hiraeth.registry/server/api/tcp"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/cluster/rpc"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/raft"
	"github.com/linkypi/hiraeth.registry/server/service"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"time"
)

type Node struct {
	selfNode   *config.NodeInfo
	rpcService *rpc.ClusterRpcService
	Config     config.NodeConfig
	Network    *network.Manager
	socket     net.Listener
	grpcServer *grpc.Server
	shutDownCh chan struct{}
}

func NewNode(config config.Config) *Node {
	return &Node{
		selfNode:   config.NodeConfig.SelfNode,
		Config:     config.NodeConfig,
		shutDownCh: make(chan struct{}),
		rpcService: rpc.NewCRpcService(config)}
}

func (n *Node) Start(conf config.Config) {

	n.Network = network.NewNetworkManager(n.selfNode.Addr)
	defer common.PrintStackTraceWithCallback(func(err any) {
		marshal, _ := json.Marshal(conf)
		common.Debugf("failed to start node: %v, config: %s", err, string(marshal))
		common.Errorf("failed to start node: %v", err)
		n.Shutdown()
	})
	//defer func() {
	//	if err := recover(); err != nil {
	//		marshal, _ := json.Marshal(conf)
	//		common.Debugf("failed to start node: %v, config: %s", err, string(marshal))
	//		common.Errorf("failed to start node: %v", err)
	//		n.Shutdown()
	//	}
	//}()
	// since grpc will enter a loop after starting
	// we need to use a channel to notify grpcServer
	// has assigned if it has been started
	rpcServerCh := make(chan *GrpcServer)
	go StartGRPCServer(n.selfNode.Addr, n.shutDownCh, rpcServerCh, func(server *grpc.Server) {
		RegisterPeerRpcService(server, n.rpcService, n.Network)
	})

	select {
	// wait for grpcServer to start, because grpc server
	// needs to be used to register the grpc service
	case server := <-rpcServerCh:
		n.grpcServer = server.Server
		n.socket = server.Socket

		break
	case <-n.shutDownCh:
		n.Shutdown()
		return
	}

	slotManager := slot.NewManager(n.selfNode.Id,
		conf.NodeConfig.DataDir, conf.ClusterConfig.NumberOfReplicas, n.shutDownCh)

	var myCluster *cluster.Cluster
	if conf.StartupMode == config.Cluster {
		myCluster = cluster.NewCluster(&conf, n.selfNode, slotManager, n.Network, n.shutDownCh)
		n.rpcService.SetCluster(myCluster)
		go myCluster.Start(n.Config.DataDir)
	} else {
		slotManager.InitSlotsForStandAlone(n.selfNode.Id, conf.ClusterConfig, n.shutDownCh)
	}

	n.startClientReceiver(conf, myCluster, slotManager)

	select {
	case <-n.shutDownCh:
		n.Shutdown()
	}
}

func (n *Node) startClientReceiver(conf config.Config, cl *cluster.Cluster, slotManager *slot.Manager) {

	codec := &common.BuildInFixedLengthCodec{Version: common.DefaultProtocolVersion}

	serviceImpl := service.RegistryImpl{Cluster: cl, StartUpMode: conf.StartupMode}
	handlerFactory := handler.NewHandlerFactory(cl, &serviceImpl)

	clientTcpServer := tcp.NewClientTcpServer(n.selfNode.GetExternalTcpAddr(), codec,
		cl, conf.StartupMode, slotManager, n.shutDownCh, handlerFactory)
	serviceImpl.OnSubEvent = clientTcpServer.PublishServiceChanged
	go clientTcpServer.Start(n.selfNode.Id)

	clientRestServer := http.NewClientRestServer(n.selfNode.GetExternalHttpAddr(), slotManager, handlerFactory, n.shutDownCh)
	go clientRestServer.Start()
}

func RegisterPeerRpcService(grpcServer *grpc.Server, clusterService *rpc.ClusterRpcService, network *network.Manager) {
	pb.RegisterClusterServiceServer(grpcServer, clusterService)
	raft.RegisterRaftTransportService(grpcServer, network)
}

func (n *Node) Shutdown() {
	common.Info("shutting down the server.")

	n.grpcServer.GracefulStop()
	n.socket.Close()

	close(n.shutDownCh)

	// wait for the server to shut down
	time.Sleep(time.Second)
	common.Info("server is down gracefully.")
	os.Exit(0)
}

type GrpcServer struct {
	Server *grpc.Server
	Socket net.Listener
}

var defaultEnforcementPolicy = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var defaultKeepaliveServerParameters = keepalive.ServerParameters{
	// If a client is idle for 15 seconds, send a GOAWAY
	MaxConnectionIdle: 15 * time.Second,
	// If any connection is alive for more than 30 seconds, send a GOAWAY
	MaxConnectionAge: 30 * time.Second,
	// Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	MaxConnectionAgeGrace: 5 * time.Second,
	// Ping the client if it is idle for 5 seconds to ensure the connection is still active
	Time: 5 * time.Second,
	// Wait 1 second for the ping ack before assuming the connection is dead
	Timeout: 1 * time.Second,
}

func StartGRPCServer(addr string, shutDownCh chan struct{}, serverCh chan *GrpcServer, register func(*grpc.Server)) {
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		common.Errorf("failed to listen: %v", err)
		shutDownCh <- struct{}{}
		return
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(defaultEnforcementPolicy),
		grpc.KeepaliveParams(defaultKeepaliveServerParameters))
	reflection.Register(grpcServer)
	register(grpcServer)
	// notify if grpcServer has been created
	serverCh <- &GrpcServer{
		Server: grpcServer,
		Socket: sock,
	}

	// start grpc server，enter an infinite loop after the startup is complete
	if err := grpcServer.Serve(sock); err != nil {
		common.Errorf("grpc server failed to serve: %v", err)
		shutDownCh <- struct{}{}
	}
}

func StartGRPCServerWithParameters(addr string, knp keepalive.EnforcementPolicy, ksp keepalive.ServerParameters,
	shutDownCh chan struct{}, serverCh chan *GrpcServer) {
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		common.Errorf("failed to listen: %v", err)
		close(shutDownCh)
		return
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(knp), grpc.KeepaliveParams(ksp))
	reflection.Register(grpcServer)

	// notify if grpcServer has been created
	serverCh <- &GrpcServer{
		Server: grpcServer,
		Socket: sock,
	}

	// start grpc server，enter an infinite loop after the startup is complete
	if err := grpcServer.Serve(sock); err != nil {
		common.Errorf("grpc server failed to serve: %v", err)
		close(shutDownCh)
	}
}
