package cluster

import (
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
)

type Syner struct {
	cluster  *Cluster
	workPool *goroutine.Pool
}

func NewSyner(cluster *Cluster) *Syner {
	return &Syner{
		cluster:  cluster,
		workPool: goroutine.Default(),
	}
}

// MaybeForwardToReplicas After the primary shard data is updated, it needs to be synchronized asynchronously to its replica shard
func (s *Syner) MaybeForwardToReplicas(request common.Request, response common.Response) {
	_ = s.workPool.Submit(func() {
		s.maybeForwardToReplicas(request, response)
	})
}
func (s *Syner) MaybeForwardToReplicasForGRpc(req *pb.ForwardCliRequest, response *pb.ForwardCliResponse) {
	_ = s.workPool.Submit(func() {
		s.maybeForwardToReplicasForGRpc(req, response)
	})
}

func (s *Syner) maybeForwardToReplicasForGRpc(req *pb.ForwardCliRequest, response *pb.ForwardCliResponse) {
	if response.ErrorType != pb.ErrorType_None {
		return
	}

	switch req.RequestType {
	case pb.RequestType_Register:
		request := &pb.RegisterRequest{}
		err := common.DecodeToPb(req.Payload, request)
		if err != nil {
			common.Warnf("failed to forward replicas, decode register request error, %v", err)
		}
		s.forwardToReplicasForGRpc(req, request.ServiceName)

	case pb.RequestType_Heartbeat:
		request := &pb.HeartbeatRequest{}
		err := common.DecodeToPb(req.Payload, request)
		if err != nil {
			common.Warnf("failed to forward replicas, decode register request error, %v", err)
		}
		s.forwardToReplicasForGRpc(req, request.ServiceName)
	}
}

func (s *Syner) maybeForwardToReplicas(request common.Request, response common.Response) {
	if !response.Success || response.ErrorType != uint8(pb.ErrorType_None) {
		return
	}

	switch request.RequestType {
	case common.Register:
		req := &pb.RegisterRequest{}
		err := common.DecodeToPb(request.Payload, req)
		if err != nil {
			common.Warnf("failed to forward replicas, decode register request error, %v", err)
		}

		s.forwardToReplicas(request, req.ServiceName)

		return
	case common.Heartbeat:
		req := &pb.HeartbeatRequest{}
		err := common.DecodeToPb(request.Payload, req)
		if err != nil {
			common.Warnf("failed to forward replicas, decode register request error, %v", err)
		}

		s.forwardToReplicas(request, req.ServiceName)

		return
	default:
		common.Debugf("no need to forward to replicas, request type: %s", request.RequestType.String())
	}
}

func (s *Syner) forwardToReplicasForGRpc(request *pb.ForwardCliRequest, serviceName string) {
	payload := request.Payload
	bucketIndex := common.GetBucketIndex(serviceName)
	serverIds, err := s.cluster.MetaData.GetReplicaServerIds(bucketIndex)
	if err != nil {
		common.Warnf("failed to find replica server ids for service %s: %s", serviceName, err)
	}
	for _, nodeId := range serverIds {
		if s.cluster.SelfNode.Id == nodeId {
			continue
		}
		res, err := s.cluster.ForwardRequest(nodeId, request.RequestType, true, payload)
		if err != nil {
			common.Warnf("failed to forward request [%s] to node %s: %s", request.RequestType.String(), nodeId, err)
			continue
		}

		if res.ErrorType != pb.ErrorType_None {
			common.Warnf("failed to forward request [%s] to node %s: %s", request.RequestType.String(), nodeId, res.ErrorType.String())
			continue
		}
		common.Debugf("forward request [%s] to node %s successfully", request.RequestType.String(), nodeId)
	}
}

func (s *Syner) forwardToReplicas(request common.Request, serviceName string) {
	reqType := common.ConvertReqType(request.RequestType)
	payload := request.Payload
	bucketIndex := common.GetBucketIndex(serviceName)
	serverIds, err := s.cluster.MetaData.GetReplicaServerIds(bucketIndex)
	if err != nil {
		common.Warnf("failed to find replica server ids for service %s: %s", serviceName, err)
	}

	for _, nodeId := range serverIds {
		if s.cluster.SelfNode.Id == nodeId {
			continue
		}
		res, err := s.cluster.ForwardRequest(nodeId, reqType, true, payload)
		if err != nil {
			common.Warnf("failed to forward request [%s] to node %s: %s", reqType.String(), nodeId, err)
			continue
		}

		if res.ErrorType != pb.ErrorType_None {
			common.Warnf("failed to forward request [%s] to node %s: %s", reqType.String(), nodeId, res.ErrorType.String())
			continue
		}
		common.Debugf("forward request [%s] to node %s successfully", reqType.String(), nodeId)
	}
}
