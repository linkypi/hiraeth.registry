package service

import (
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
)

func RegisterRpcService(grpcServer *grpc.Server, config config.NodeConfig, manager *network.NetworkManager) {
	service := ClusterService{nodeConfig: config}
	pb.RegisterClusterServiceServer(grpcServer, &service)
	raft.RegisterRaftTransportService(grpcServer, manager)
}
