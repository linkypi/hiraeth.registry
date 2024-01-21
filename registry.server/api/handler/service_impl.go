package handler

import (
	"encoding/json"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"github.com/sirupsen/logrus"
	"strconv"
)

// ServiceImpl was added to facilitate the unification of TCP and HTTP processing logic
type ServiceImpl struct {
	Log            *logrus.Logger
	ServiceHandler *ServiceImpl
	SlotManager    *slot.Manager
	Cluster        *cluster.Cluster
	StartUpMode    config.StartUpMode
	OnSubEvent     slot.SubscribeCallback
}

func (s *ServiceImpl) Heartbeat(serviceName, ip string, port int) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}

	_, err := s.Cluster.FindRouteAndExecOrForward(serviceName, func(bucket *slot.Bucket) (any, error) {
		bucket.Heartbeat(serviceName, ip, port)
		return nil, nil
	}, func() (pb.RequestType, []byte, error) {
		// forwarding to the right node
		heartRequest := pb.HeartbeatRequest{ServiceName: serviceName, ServiceIp: ip, ServicePort: int32(port)}
		payload, err := common.EncodePb(&heartRequest)
		if err != nil {
			return pb.RequestType_Unknown, nil, err
		}
		return pb.RequestType_Heartbeat, payload, nil
	})

	if err == nil {
		return nil
	}
	return err
}

func (s *ServiceImpl) FetchMetadata() (*pb.FetchMetadataResponse, error) {

	if s.Cluster.StartUpMode == config.StandAlone {
		nodeId := s.Cluster.SelfNode.Id
		shards := make(map[string]common.Shard)
		segment := common.Segment{Start: 0, End: common.SlotsCount - 1}
		shards[nodeId] = common.Shard{NodeId: nodeId, Segments: []common.Segment{segment}}

		nodes := make(map[string]string)
		nodes[nodeId] = s.Cluster.SelfNode.Ip + strconv.Itoa(s.Cluster.NodeConfig.ClientTcpPort)

		jsonBytes, err := json.Marshal(shards)
		if err != nil {
			return nil, err
		}
		return &pb.FetchMetadataResponse{Shards: string(jsonBytes), Nodes: nodes}, nil
	}

	// If it is a cluster mode, you need to check the cluster status first
	if s.Cluster.State != cluster.Active {
		s.Log.Errorf("failed to fetch meta data, cluster state not active: %v", s.Cluster.State.String())
		return &pb.FetchMetadataResponse{ErrorType: pb.ErrorType_ClusterDown}, nil
	}
	return s.doFetchMetadata()
}

func (s *ServiceImpl) FetchServiceInstance(serviceName string) (*pb.FetchServiceResponse, error) {

	res, err := s.Cluster.FindRouteAndExecOrForward(serviceName, func(bucket *slot.Bucket) (any, error) {
		return s.doGetServiceInstance(serviceName, bucket)
	}, func() (pb.RequestType, []byte, error) {
		// forwarding to the right node
		request := pb.FetchServiceResponse{ServiceName: serviceName, ServiceInstances: make([]*pb.ServiceInstance, 0)}

		payload, err := common.EncodePb(&request)
		if err != nil {
			return pb.RequestType_Unknown, nil, err
		}
		return pb.RequestType_FetchServiceInstance, payload, nil
	})

	if err == nil {
		return res.(*pb.FetchServiceResponse), nil
	}
	return nil, err
}

func (s *ServiceImpl) doFetchMetadata() (*pb.FetchMetadataResponse, error) {
	jsonBytes, err := json.Marshal(s.Cluster.MetaData.Shards)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]string)
	for _, nodeInfo := range s.Cluster.ClusterActualNodes {
		nodes[nodeInfo.Id] = nodeInfo.Ip + ":" + strconv.Itoa(nodeInfo.ExternalTcpPort)
	}
	return &pb.FetchMetadataResponse{Shards: string(jsonBytes), Nodes: nodes}, nil
}

func (s *ServiceImpl) Subscribe(serviceName, connId string) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}
	_, err := s.Cluster.FindRouteAndExecNoForward(serviceName, func(bucket *slot.Bucket) (any, error) {
		bucket.Subscribe(serviceName, connId, s.OnSubEvent)
		return nil, nil
	})
	return err
}

func (s *ServiceImpl) UnSubscribe(serviceName, addr string) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}

	_, err := s.Cluster.FindRouteAndExecNoForward(serviceName, func(bucket *slot.Bucket) (any, error) {
		bucket.Unsubscribe(serviceName, addr)
		return nil, nil
	})

	if err == nil {
		return nil
	}
	return err
}

func (s *ServiceImpl) Register(serviceName, ip string, port int) error {

	_, err := s.Cluster.FindRouteAndExecOrForward(serviceName, func(bucket *slot.Bucket) (any, error) {
		return nil, s.doRegister(bucket, serviceName, ip, port)
	}, func() (pb.RequestType, []byte, error) {
		// forwarding to the right node
		regRequest := pb.RegisterRequest{
			ServiceName: serviceName,
			ServiceIp:   ip,
			ServicePort: int32(port)}

		payload, err := common.EncodePb(&regRequest)
		if err != nil {
			return pb.RequestType_Unknown, nil, err
		}
		return pb.RequestType_Register, payload, nil
	})

	if err == nil {
		return nil
	}
	return err
}

func (s *ServiceImpl) doRegister(bucket *slot.Bucket, serviceName, serviceIp string, servicePort int) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}
	if serviceIp == "" {
		return errors.New("service ip not empty")
	}
	if servicePort <= 0 {
		return errors.New("invalid port")
	}
	bucket.Register(serviceName, serviceIp, servicePort)
	s.Log.Debugf("register service %s success -> %s:%d", serviceName, serviceIp, servicePort)
	return nil
}

func (s *ServiceImpl) doGetServiceInstance(serviceName string, bucket *slot.Bucket) (*pb.FetchServiceResponse, error) {
	instances := bucket.GetServiceInstances(serviceName)
	if instances == nil {
		return &pb.FetchServiceResponse{ServiceName: serviceName, ServiceInstances: make([]*pb.ServiceInstance, 0)}, nil
	}
	list := make([]*pb.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		item := &pb.ServiceInstance{ServiceName: serviceName, ServiceIp: instance.InstanceIp, ServicePort: int32(instance.InstancePort)}
		list = append(list, item)
	}
	return &pb.FetchServiceResponse{ServiceInstances: list, ServiceName: serviceName}, nil
}
