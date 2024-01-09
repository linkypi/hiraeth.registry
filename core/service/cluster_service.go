package service

import (
	"context"
	"github.com/linkypi/hiraeth.registry/config"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"strconv"
)

type ClusterService struct {
	nodeConfig config.NodeConfig
}

func (s *ClusterService) GetNodeInfo(ctx context.Context, req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {

	remoteNode := config.NodeInfo{
		Id:          req.NodeId,
		Ip:          req.NodeIp,
		Addr:        req.NodeIp + ":" + strconv.Itoa(int(req.GetInternalPort())),
		IsCandidate: req.IsCandidate,
	}

	// it is very likely that the information of the remote node
	// is not in the current cluster configuration, such as the newly joined node
	// in this case, the current node needs to actively connect to the node in order
	// to maintain a persistent connection between them when the node
	// has sent the command to join the cluster
	s.nodeConfig.UpdateRemoteNode(remoteNode, false)

	node := s.nodeConfig.SelfNode
	return &pb.NodeInfoResponse{
		NodeId:       node.Id,
		NodeIp:       node.Ip,
		InternalPort: uint64(node.InternalPort),
		IsCandidate:  node.IsCandidate,
	}, nil
}
