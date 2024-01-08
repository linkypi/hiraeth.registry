package service

import (
	"context"
	"hiraeth.registry/config"

	pb "hiraeth.registry/proto"
)

type InternalServiceServer struct {
	ServerConfig config.ServerConfig
}

func (s *InternalServiceServer) GetNodeInfo(ctx context.Context, req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {
	// update cluster node info by node addr
	s.ServerConfig.UpdateNode(int(req.NodeId), req.NodeIp, int(req.InternalPort))
	node := s.ServerConfig.CurrentNode
	return &pb.NodeInfoResponse{
		NodeId:       uint32(node.Id),
		NodeIp:       node.Ip,
		InternalPort: uint32(node.InternalPort),
	}, nil
}
