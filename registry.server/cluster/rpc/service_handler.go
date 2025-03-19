package rpc

import (
	"context"
	"errors"
	"fmt"
	common "github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
)

func buildNodeInfo(req *pb.NodeInfoRequest) config.NodeInfo {
	remoteNode := config.NodeInfo{
		Id:                    req.NodeId,
		Ip:                    req.NodeIp,
		Addr:                  req.NodeIp + ":" + strconv.Itoa(int(req.GetInternalPort())),
		IsCandidate:           req.IsCandidate,
		AutoJoinClusterEnable: req.AutoJoinClusterEnable,
		ExternalTcpPort:       int(req.ExternalTcpPort),
		ExternalHttpPort:      int(req.ExternalHttpPort),
	}
	return remoteNode
}

func buildNodeInfoForJoinReq(req *pb.JoinClusterRequest) config.NodeInfo {
	remoteNode := config.NodeInfo{
		Id:                    req.NodeId,
		Addr:                  req.NodeAddr,
		IsCandidate:           req.IsCandidate,
		AutoJoinClusterEnable: req.AutoJoinClusterEnable,
	}
	return remoteNode
}

func (c *ClusterRpcService) replyNodeInfo(req *pb.NodeInfoRequest) (*pb.NodeInfoResponse, error) {

	remoteNode := buildNodeInfo(req)
	node := c.cluster.SelfNode

	var appliedIndex uint64
	if c.cluster.Raft != nil {
		appliedIndex = c.cluster.Raft.AppliedIndex()
	}
	mode := pb.StartupMode_Cluster
	if c.config.StartupMode == config.StandAlone {
		mode = pb.StartupMode_StandAlone
	}
	response := &pb.NodeInfoResponse{
		NodeId:                node.Id,
		NodeIp:                node.Ip,
		InternalPort:          uint64(node.InternalPort),
		IsCandidate:           node.IsCandidate,
		AutoJoinClusterEnable: node.AutoJoinClusterEnable,
		ExternalHttpPort:      uint32(node.ExternalHttpPort),
		ExternalTcpPort:       uint32(node.ExternalTcpPort),
		StartupMode:           mode,
		AppliedIndex:          appliedIndex,
	}

	if c.config.StartupMode == config.StandAlone {
		//errors.New("the node is in stand-alone mode, and the information of the node cannot be obtained")
		return response, nil
	}

	// it is very likely that the information of the remote node
	// is not in the current cluster configuration, such as the newly joined node
	// in this case, the current node needs to actively connect to the node in order
	// to maintain a persistent connection between them when the node
	// has sent the command to join the cluster
	err := c.cluster.UpdateRemoteNode(remoteNode, *c.cluster.SelfNode, false)
	if err != nil {
		return &pb.NodeInfoResponse{}, err
	}

	return response, nil
}

func (c *ClusterRpcService) addNodeToCluster(remoteNode config.NodeInfo) error {
	cls := c.cluster
	if cls.Raft == nil {
		// this is rarely the case, unless a stand-alone service starts
		common.Errorf("[cluster] Raft is not initialized, can't add node to cluster, node id %s, addr : %s", remoteNode.Id, remoteNode.Addr)
		return errors.New("raft is not initialized, can't add node to cluster")
	}
	addr, leaderId := cls.Raft.LeaderWithID()
	if addr == "" || leaderId == "" {
		common.Errorf("[cluster] leader not found, can't add node to cluster, "+
			"current node state: %s, cluster state: %s", cls.Raft.State().String(), cls.State.String())
		return errors.New("leader not found, can't add node to cluster")
	}

	// only the leader node has the permission to add nodes, otherwise the addition fails
	if cls.IsLeader() {
		return cls.Leader.AddNewNode(&remoteNode)
	}
	// forward the request to the leader node for addition
	common.Infof("[cluster] forwarding the request to the leader node for addition, node id: %s, addr: %s", leaderId, addr)
	rpcClient := cls.GetInterRpcClient(string(leaderId))
	request := &pb.JoinClusterRequest{
		NodeId:                remoteNode.Id,
		NodeAddr:              remoteNode.Addr,
		IsCandidate:           remoteNode.IsCandidate,
		AutoJoinClusterEnable: true,
	}
	_, err := (*rpcClient).ForwardJoinClusterRequest(context.Background(), request)
	if err != nil {
		common.Error("[cluster] failed to forward the request to the leader node for addition,"+
			" node id: %s, addr: %s, error: %s", leaderId, addr, err.Error())
		return err
	}
	common.Infof("added node to cluster success: %s, %s", remoteNode.Id, remoteNode.Addr)
	return nil
}

func (c *ClusterRpcService) replyFollowerInfo(*pb.FollowerInfoRequest) (response *pb.FollowerInfoResponse, err error) {
	return &pb.FollowerInfoResponse{
		Term:         c.cluster.Leader.Term,
		LeaderId:     c.cluster.Leader.Id,
		ClusterState: c.cluster.State.String(),
		NodeId:       c.cluster.SelfNode.Id,
		NodeAddr:     c.cluster.SelfNode.Addr,
	}, nil
}

func (c *ClusterRpcService) handleLeadershipTransfer(req *pb.TransferRequest) (*pb.TransferResponse, error) {
	//if req.Status == pb.TransferStatus_Transitioning && c.cluster.State > cluster.Transitioning {
	//	common.Errorf("[follower] failed to transfer leadership to [%s] status, the cluster state not match: %s, "+
	//		"shoule be %s", req.Status.String(), c.cluster.State.String(), cluster.Active.String())
	//	response := pb.TransferResponse{ErrorType: pb.ErrorType_ClusterStateNotMatch, ClusterState: c.cluster.State.String()}
	//	return &response, nil
	//}
	leaderId, _, term := c.cluster.GetLeaderInfoFromRaft()
	if req.LeaderId != leaderId {
		common.Errorf("[follower] failed to transfer leadership to [%s] status, the raft leader id not match,"+
			" current leader id: %s, remote leader id: %s", req.Status.String(), leaderId, req.LeaderId)
		return &pb.TransferResponse{ErrorType: pb.ErrorType_LeaderIdNotMatch, ClusterState: c.cluster.State.String()}, nil
	}
	if req.Term != term {
		common.Errorf("[follower] failed to transfer leadership to [%s] status, the raft term not match, "+
			"current leader id: %d, remote leader id: %d", req.Status.String(), term, req.Term)
		return &pb.TransferResponse{ErrorType: pb.ErrorType_LeaderIdNotMatch, ClusterState: c.cluster.State.String()}, nil
	}
	if req.Status == pb.TransferStatus_Completed && c.cluster.State > cluster.Transitioning {
		common.Errorf("[follower] failed to transfer leadership to [%s] status, the cluster state not match: %s, "+
			"shoule be %s", req.Status.String(), c.cluster.State.String(), cluster.Transitioning.String())
		return &pb.TransferResponse{ErrorType: pb.ErrorType_ClusterStateNotMatch, ClusterState: c.cluster.State.String()}, nil
	}

	defer func() {
		if e := recover(); e != nil {
			common.Errorf("transfer leadership to %s panic: %v", req.Status.String(), e)
		}
	}()
	common.Infof("[follower] update leader to  [id:%s][term:%d][%s]", req.LeaderId, req.Term, req.Addr)
	c.cluster.UpdateLeader(req.Term, req.LeaderId, req.Addr)

	if req.Status == pb.TransferStatus_Transitioning {
		c.cluster.SetState(cluster.Transitioning)
		common.Infof("[follower] the cluster is unhealth, pause receiving client write requests")
	}
	if req.Status == pb.TransferStatus_Completed {
		c.cluster.SetState(cluster.Active)
		common.Infof("[follower] the cluster is healthy, start receiving client requests")
	}
	return &pb.TransferResponse{ErrorType: pb.ErrorType_None}, nil
}

func (c *ClusterRpcService) joinNodeToCluster(req *pb.JoinClusterRequest) (*emptypb.Empty, error) {
	remoteNode := buildNodeInfoForJoinReq(req)
	if !c.cluster.IsActive() {
		common.Infof("the cluster is not active, can not add remote node to the cluster, "+
			"id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
		return &emptypb.Empty{}, errors.New("the cluster is not active")
	}
	existNode := c.cluster.RaftExistNode(remoteNode.Id)
	if existNode {
		common.Infof("raft cluster has already exist node, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
		return &emptypb.Empty{}, nil
	}
	common.Infof("the cluster is active, remote node id: %s, addr: %s, raft config exist: %v",
		remoteNode.Id, remoteNode.Addr, existNode)
	if !existNode && remoteNode.AutoJoinClusterEnable {
		err := c.addNodeToCluster(remoteNode)
		if err != nil {
			return &emptypb.Empty{}, err
		}
		return &emptypb.Empty{}, nil
	}

	return &emptypb.Empty{}, nil
}

func (c *ClusterRpcService) fetchServiceInstances(req *pb.FetchServiceRequest, err error) (*pb.FetchServiceResponse, error) {

	bucketIndex := common.GetBucketIndex(req.ServiceName)

	// If it is a stand-alone mode, you can fetch directly
	if c.config.StartupMode == config.StandAlone {

		return nil, nil
	}

	nodeId, err := c.cluster.GetNodeIdByIndex(bucketIndex)
	if c.cluster.State != cluster.Active {
		common.Errorf("failed to register service: %s, cluster state not active: %v", req.ServiceName, c.cluster.State.String())
		return nil, errors.New("cluster state not active: " + c.cluster.State.String())
	}

	if nodeId == c.cluster.SelfNode.Id {

	}
	bucket := c.cluster.SlotManager.GetSlotByIndex(bucketIndex)
	instances := bucket.GetServiceInstances(req.ServiceName)
	serviceInstances := make([]*pb.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		serviceInstance := pb.ServiceInstance{
			ServiceName: instance.ServiceName,
			ServiceIp:   instance.InstanceIp,
			ServicePort: int32(instance.InstancePort),
		}
		serviceInstances = append(serviceInstances, &serviceInstance)
	}
	response := &pb.FetchServiceResponse{
		ServiceName:      req.ServiceName,
		ServiceInstances: serviceInstances,
	}
	return response, nil
}

func (c *ClusterRpcService) handleMetadata(req *pb.PublishMetadataRequest, response *pb.PublishMetadataResponse, err error) (*pb.PublishMetadataResponse, error) {
	if c.cluster.State != cluster.Transitioning {
		common.Errorf("cluster metadata reception failed, the cluster state not match: %s, "+
			"shoule be %s", c.cluster.State.String(), cluster.Transitioning.String())
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_ClusterStateNotMatch, ClusterState: c.cluster.State.String()}, nil
	}
	leaderId, _, term := c.cluster.GetLeaderInfoFromRaft()
	if req.LeaderId != leaderId {
		common.Errorf("cluster metadata reception failed, leader id not match, "+
			"remote leader id: %s, current leader id: %s", req.LeaderId, leaderId)
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_LeaderIdNotMatch, LeaderId: leaderId}, nil
	}
	if req.Term != term {
		common.Errorf("cluster metadata reception failed, term not match, "+
			"remote term: %d, current leader term: %d", req.Term, term)
		return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_TermNotMatch, Term: term}, nil
	}
	defer func() {
		if e := recover(); e != nil {
			common.Errorf("transfer leadership panic: %v", e)
			response = nil
			err = errors.New(fmt.Sprintf("%v", e))
		}
	}()

	err = c.cluster.ApplyClusterMetaData(err, req)
	if err != nil {
		return nil, err
	}

	return &pb.PublishMetadataResponse{ErrorType: pb.ErrorType_None}, nil
}
