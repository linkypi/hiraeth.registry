package service

import (
	"github.com/linkypi/hiraeth.registry/config"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
)

func RegisterRpcService(grpcServer *grpc.Server, config config.NodeConfig) {
	service := ClusterService{nodeConfig: config}
	pb.RegisterClusterServiceServer(grpcServer, &service)
}
