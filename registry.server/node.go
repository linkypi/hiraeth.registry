package main

import (
	"encoding/json"
	"github.com/linkypi/hiraeth.registry/server/api/http"
	"github.com/linkypi/hiraeth.registry/server/api/tcp"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/cluster/rpc"
	"github.com/linkypi/hiraeth.registry/server/common"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/log"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/linkypi/hiraeth.registry/server/raft"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"os"
	"time"
)

type Node struct {
	log        *logrus.Logger
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
		rpcService: rpc.NewCRpcService(config),
		log:        log.Log}
}

func (n *Node) Start(conf config.Config) {

	n.Network = network.NewNetworkManager(n.selfNode.Addr)
	defer func() {
		if err := recover(); err != nil {
			marshal, _ := json.Marshal(conf)
			n.log.Debugf("faile to start node: %v, config: %s", err, string(marshal))
			n.log.Errorf("faile to start node: %v", err)
			n.Shutdown()
		}
	}()
	// since grpc will enter a loop after starting
	// we need to use a channel to notify grpcServer
	// has assigned if it has been started
	rpcServerCh := make(chan *common.GrpcServer)
	go common.StartGRPCServer(n.selfNode.Addr, n.shutDownCh, rpcServerCh, func(server *grpc.Server) {
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

func (n *Node) startClientReceiver(conf config.Config, myCluster *cluster.Cluster, slotManager *slot.Manager) {
	clientTcpServer := tcp.NewClientTcpServer(n.selfNode.GetExternalTcpAddr(),
		myCluster, conf.StartupMode, slotManager, n.shutDownCh)
	go clientTcpServer.Start(n.selfNode.Id)

	clientRestServer := http.NewClientRestServer(n.selfNode.GetExternalHttpAddr(), slotManager,
		myCluster, conf.StartupMode, n.shutDownCh)
	go clientRestServer.Start()
}

func RegisterPeerRpcService(grpcServer *grpc.Server, clusterService *rpc.ClusterRpcService, network *network.Manager) {
	pb.RegisterClusterServiceServer(grpcServer, clusterService)
	raft.RegisterRaftTransportService(grpcServer, network)
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
