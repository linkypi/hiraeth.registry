package service

import (
	"github.com/linkypi/hiraeth.registry/core/cluster/rpc"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
)

func RegisterRpcService(grpcServer *grpc.Server, clusterService *rpc.ClusterRpcService, network *network.NetworkManager) {
	pb.RegisterClusterServiceServer(grpcServer, clusterService)
	raft.RegisterRaftTransportService(grpcServer, network)
}
