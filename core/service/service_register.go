package service

import (
	"github.com/linkypi/hiraeth.registry/core/cluster"
	"github.com/linkypi/hiraeth.registry/core/network"
	"github.com/linkypi/hiraeth.registry/core/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
)

func RegisterRpcService(grpcServer *grpc.Server, cluster *cluster.Cluster, network *network.NetworkManager) {
	pb.RegisterClusterServiceServer(grpcServer, cluster)
	raft.RegisterRaftTransportService(grpcServer, network)
}
