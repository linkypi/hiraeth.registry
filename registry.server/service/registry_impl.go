package service

import (
	"encoding/json"
	"errors"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/slot"
	"strconv"
)

// RegistryImpl was added to facilitate the unification of TCP and HTTP processing logic
type RegistryImpl struct {
	syner       *cluster.Syner
	Cluster     *cluster.Cluster
	StartUpMode config.StartUpMode
	OnSubEvent  slot.SubscribeCallback
}

func (s *RegistryImpl) FetchMetadata() (*pb.FetchMetadataResponse, error) {

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
		common.Errorf("failed to fetch meta data, cluster state not active: %v", s.Cluster.State.String())
		return &pb.FetchMetadataResponse{ErrorType: pb.ErrorType_ClusterDown}, nil
	}
	return s.doFetchMetadata()
}

func (s *RegistryImpl) doFetchMetadata() (*pb.FetchMetadataResponse, error) {
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

func (s *RegistryImpl) Subscribe(bucket *slot.Bucket, serviceName, connId string) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}

	bucket.Subscribe(serviceName, connId, s.OnSubEvent)
	return nil
}

func (s *RegistryImpl) UnSubscribe(bucket *slot.Bucket, serviceName, addr string) error {
	if serviceName == "" {
		return errors.New("service name not empty")
	}

	bucket.Unsubscribe(serviceName, addr)

	return nil
}

func (s *RegistryImpl) DoHeartbeat(bucket *slot.Bucket, serviceName, serviceIp string, servicePort int) error {
	bucket.Heartbeat(serviceName, serviceIp, servicePort)
	return nil
}

func (s *RegistryImpl) DoRegister(bucket *slot.Bucket, serviceName, serviceIp string, servicePort int) error {
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
	common.Debugf("register service %s success -> %s:%d", serviceName, serviceIp, servicePort)
	return nil
}

func (s *RegistryImpl) DoGetServiceInstance(serviceName string, bucket *slot.Bucket) (*pb.FetchServiceResponse, error) {
	instances := bucket.GetServiceInstances(serviceName)
	if instances == nil || len(instances) == 0 {
		return &pb.FetchServiceResponse{ServiceName: serviceName, ServiceInstances: make([]*pb.ServiceInstance, 0)}, nil
	}
	list := make([]*pb.ServiceInstance, 0, len(instances))
	for _, instance := range instances {
		item := &pb.ServiceInstance{ServiceName: serviceName, ServiceIp: instance.InstanceIp, ServicePort: int32(instance.InstancePort)}
		list = append(list, item)
	}
	return &pb.FetchServiceResponse{ServiceInstances: list, ServiceName: serviceName}, nil
}
