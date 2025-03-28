package rpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"google.golang.org/protobuf/proto"
)

func (c *ClusterRpcService) ForwardClientRequest(ctx context.Context, req *pb.ForwardCliRequest) (res *pb.ForwardCliResponse, e error) {
	defer func() {
		if err := recover(); err != nil {
			msg := fmt.Sprintf("failed handle forward reqeuest: %v", err)
			common.Error(msg)
			e = errors.New(msg)
		}
	}()
	forwardRes := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}

	common.Debugf(" handle forward reqeuest: %s ", req.RequestType.String())
	// Check whether they belong to the same cluster
	errorType := c.cluster.CheckClusterInfo(req.ClusterId, req.LeaderId, req.Term)
	if errorType != pb.ErrorType_None {
		forwardRes.ErrorType = errorType
		common.Errorf("failed handle forward %s reqeuest: %v", req.RequestType.String(), errorType)
		return forwardRes, nil
	}

	response, err := c.doLogic(req, errorType, res, forwardRes)
	if response.ErrorType == pb.ErrorType_None && !req.SyncReplica {
		c.syner.MaybeForwardToReplicasForGRpc(req, response)
	}
	return response, err
}

func (c *ClusterRpcService) doLogic(req *pb.ForwardCliRequest, errorType pb.ErrorType, res *pb.ForwardCliResponse, forwardRes *pb.ForwardCliResponse) (*pb.ForwardCliResponse, error) {
	if req.RequestType == pb.RequestType_Register {
		var request pb.RegisterRequest
		err := proto.Unmarshal(req.Payload, &request)
		if err != nil {
			common.Errorf("failed handle forward %s reqeuest: %v", req.RequestType.String(), err)
			return nil, err
		}
		if req.SyncReplica {
			res = c.doRegisterService(&request)
			return res, nil
		}
		// Check whether the metadata of the cluster has changed
		errorType = c.cluster.CheckNodeRouteForServiceName(request.ServiceName)
		if errorType == pb.ErrorType_None {
			res = c.doRegisterService(&request)
			return res, nil
		}
		forwardRes.ErrorType = errorType
		return forwardRes, nil
	}

	if req.RequestType == pb.RequestType_Heartbeat {
		var request pb.HeartbeatRequest
		err := proto.Unmarshal(req.Payload, &request)
		if err != nil {
			common.Errorf("failed handle forward %s reqeuest: %v", req.RequestType.String(), err)
			return nil, err
		}
		if req.SyncReplica {
			res = c.doServiceHeartbeat(&request)
			return res, nil
		}
		// Check whether the metadata of the cluster has changed
		errorType = c.cluster.CheckNodeRouteForServiceName(request.ServiceName)
		if errorType == pb.ErrorType_None {
			res = c.doServiceHeartbeat(&request)
			return res, nil
		}
		forwardRes.ErrorType = errorType
		return forwardRes, nil
	}

	// Subscription requests do not need to be forwarded because the server needs to be directly
	// connected to the client to send service change notifications
	//if req.RequestType == pb.RequestType_Subscribe {
	//}

	return &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_UnknownError,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}, nil
}

func (c *ClusterRpcService) doServiceHeartbeat(req *pb.HeartbeatRequest) *pb.ForwardCliResponse {
	response := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}

	bucketIndex := common.GetBucketIndex(req.ServiceName)
	bucket := c.cluster.SlotManager.GetSlotByIndex(bucketIndex)
	bucket.Heartbeat(req.ServiceName, req.ServiceIp, int(req.ServicePort))
	return response
}

func (c *ClusterRpcService) doRegisterService(req *pb.RegisterRequest) *pb.ForwardCliResponse {

	response := &pb.ForwardCliResponse{
		ErrorType: pb.ErrorType_None,
		ClusterId: c.cluster.ClusterId,
		LeaderId:  c.cluster.Leader.Id,
		Term:      c.cluster.Leader.Term,
	}

	bucketIndex := common.GetBucketIndex(req.ServiceName)
	bucket := c.cluster.SlotManager.GetSlotByIndex(bucketIndex)
	bucket.Register(req.ServiceName, req.ServiceIp, int(req.ServicePort))
	return response
}
