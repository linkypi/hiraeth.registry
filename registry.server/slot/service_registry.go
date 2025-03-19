package slot

import (
	common "github.com/linkypi/hiraeth.registry/common"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/panjf2000/gnet/pkg/pool/goroutine"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// SubsServicePrefix Stores a list of client addresses for the service
	SubsServicePrefix = "subs_service_"

	InstanceIdPrefix = "instance_id_"
	ServiceKeyPrefix = "service_instances_"
)

type ServiceRegistry struct {
	sync.Map
	workerPool *goroutine.Pool
	trigger    SubscribeCallback
}

type SubscribeCallback func(connIds []string, serviceName string, instances []common.ServiceInstance)

func NewServiceRegistry(clusterConfig config.ClusterConfig, shutdownCh chan struct{}) *ServiceRegistry {
	serviceRegistry := ServiceRegistry{workerPool: goroutine.Default()}
	go checkServiceInstanceStatePeriod(clusterConfig, shutdownCh, &serviceRegistry)
	return &serviceRegistry
}

func checkServiceInstanceStatePeriod(clusterConfig config.ClusterConfig, shutdownCh chan struct{}, serviceRegistry *ServiceRegistry) {
	printTime := time.Now()
	for {
		select {
		case <-shutdownCh:
			return
		default:
			// If it is not updated for more than 15 seconds, it is marked as unhealthy
			// and if it is not updated for more than 30 seconds, the service instance is removed
			time.Sleep(time.Second * 5)

			serviceRegistry.Range(func(key, value interface{}) bool {
				if !strings.Contains(key.(string), InstanceIdPrefix) {
					return true
				}
				instance := value.(common.ServiceInstance)

				if time.Now().Sub(printTime).Seconds() > 30 {
					common.Debugf("service instance %s:%d lastest heartbeat time: %s, State: %s",
						instance.InstanceIp, instance.InstancePort,
						instance.LastHeartbeatTime, instance.State.String())
					printTime = time.Now()
				}

				duration := time.Since(instance.LastHeartbeatTime).Seconds()
				if duration > float64(clusterConfig.ServiceInstanceRemoveTimeoutSec) {

					common.Warnf("service instance %s:%d has expired, removed from cache.",
						instance.InstanceIp, instance.InstancePort)

					serviceRegistry.Delete(key)
					instances := serviceRegistry.removeFromInstances(instance)

					// Notify all clients that have subscribed to the service
					serviceRegistry.maybePublishChangedService(instance.ServiceName, instances)
					// Remove the address list when the notification is complete
					removeInstanceAddr(instance, serviceRegistry)

					return true
				}
				if duration > float64(clusterConfig.ServiceInstanceUnhealthyTimeoutSec) {
					common.Warnf("service instance %s:%d is unhealth.", instance.InstanceIp, instance.InstancePort)
					instance.State = common.UnHealthy
					serviceRegistry.Store(key, instance)
					return true
				}
				instance.State = common.Healthy
				serviceRegistry.Store(key, instance)
				return true
			})
		}
	}
}

func removeInstanceAddr(instance common.ServiceInstance, serviceRegistry *ServiceRegistry) {
	key := SubsServicePrefix + instance.ServiceName
	listenerMap, ok := serviceRegistry.Load(key)
	if ok {
		subMap := listenerMap.(map[string]string)
		if len(subMap) > 0 {
			delete(subMap, instance.InstanceIp+":"+strconv.Itoa(instance.InstancePort))
			serviceRegistry.Store(key, subMap)
		}
	}
}

func (s *ServiceRegistry) removeFromInstances(instance common.ServiceInstance) []common.ServiceInstance {
	listKey := ServiceKeyPrefix + instance.ServiceName
	list, ok := s.Load(listKey)
	if !ok {
		return nil
	}
	instances := list.([]common.ServiceInstance)
	if instances == nil {
		return nil
	}
	for i := 0; i < len(instances); i++ {
		serviceInstance := instances[i]
		if serviceInstance.InstanceIp == instance.InstanceIp && serviceInstance.InstancePort == instance.InstancePort {
			instances = append(instances[:i], instances[i+1:]...)
			s.Store(listKey, instances)
			common.Debugf("remove service instance %s:%d from service %s.", instance.InstanceIp, instance.InstancePort, instance.ServiceName)
			return instances
		}
	}
	return nil
}

func (s *ServiceRegistry) Heartbeat(serviceName, ip string, port int) {
	instanceId := InstanceIdPrefix + serviceName + "/" + ip + ":" + strconv.Itoa(port)
	instances, ok := s.Load(instanceId)
	if !ok {
		return
	}
	instance := instances.(common.ServiceInstance)
	instance.LastHeartbeatTime = time.Now()

	s.Store(instanceId, instance)
}

func (s *ServiceRegistry) Subscribe(serviceName string, connId string, trigger SubscribeCallback) {

	s.trigger = trigger
	key := SubsServicePrefix + serviceName
	addrMap := make(map[string]string)
	list, ok := s.Load(key)
	if !ok {
		addrMap[connId] = connId
		s.Store(key, addrMap)
		s.maybePublishChangedService(serviceName, nil)
		return
	}
	listMap := list.(map[string]string)
	_, ok = listMap[connId]
	if ok {
		return
	}
	listMap[connId] = connId
	s.Store(key, listMap)
	s.maybePublishChangedService(serviceName, nil)
}

func (s *ServiceRegistry) Unsubscribe(serviceName string, connId string) {
	key := SubsServicePrefix + serviceName
	listeners, ok := s.Load(key)
	if ok {
		listMap := listeners.(map[string]string)
		_, ok = listMap[connId]
		if ok {
			delete(listMap, connId)
			return
		}
	}
}

func (s *ServiceRegistry) GetServiceInstances(serviceName string) []common.ServiceInstance {
	value, ok := s.Load(ServiceKeyPrefix + serviceName)
	if !ok {
		return nil
	}
	return value.([]common.ServiceInstance)
}

func (s *ServiceRegistry) Register(serviceName string, instanceIp string, instancePort int) {
	instance := common.ServiceInstance{
		ServiceName:       serviceName,
		InstanceIp:        instanceIp,
		InstancePort:      instancePort,
		State:             common.Healthy,
		LastHeartbeatTime: time.Now(),
	}

	instances, ok := s.Load(ServiceKeyPrefix + serviceName)
	if !ok {
		list := make([]common.ServiceInstance, 0, 16)
		list = append(list, instance)
		s.Store(ServiceKeyPrefix+serviceName, list)
		s.Store(InstanceIdPrefix+instance.GetInstanceId(), instance)

		s.maybePublishChangedService(serviceName, list)
		return
	}

	var exist bool
	list := instances.([]common.ServiceInstance)
	for _, service := range list {
		if service.GetInstanceId() == instance.GetInstanceId() {
			exist = true
			service.LastHeartbeatTime = time.Now()
		}
	}
	if !exist {
		list = append(list, instance)
		s.Store(InstanceIdPrefix+instance.GetInstanceId(), instance)

		// Notify all clients that have subscribed to the service
		s.maybePublishChangedService(serviceName, list)
	}
	s.Store(ServiceKeyPrefix+serviceName, list)
}

func (s *ServiceRegistry) maybePublishChangedService(serviceName string, list []common.ServiceInstance) {
	key := SubsServicePrefix + serviceName
	listenerMap, ok := s.Load(key)
	if ok {
		subMap := listenerMap.(map[string]string)
		if len(subMap) > 0 {
			connIds := make([]string, 0, len(subMap))
			for _, addr := range subMap {
				connIds = append(connIds, addr)
			}
			if list == nil || len(list) == 0 {
				value, ok := s.Load(ServiceKeyPrefix + serviceName)
				if ok {
					list = value.([]common.ServiceInstance)
				}
			}
			if len(list) == 0 {
				return
			}
			go s.trigger(connIds, serviceName, list)
		}
	}
}

func NewServiceInstance(serviceName string, instanceIp string, instancePort int) *common.ServiceInstance {
	return &common.ServiceInstance{
		ServiceName:       serviceName,
		InstanceIp:        instanceIp,
		InstancePort:      instancePort,
		State:             common.Healthy,
		LastHeartbeatTime: time.Now(),
	}
}
