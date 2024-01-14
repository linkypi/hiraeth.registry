package service

import (
	"github.com/linkypi/hiraeth.registry/cluster/rpc"
	"github.com/linkypi/hiraeth.registry/network"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/linkypi/hiraeth.registry/raft"
	"google.golang.org/grpc"
)

func RegisterRpcService(grpcServer *grpc.Server, clusterService *rpc.ClusterRpcService, network *network.Manager) {
	pb.RegisterClusterServiceServer(grpcServer, clusterService)
	raft.RegisterRaftTransportService(grpcServer, network)
}
