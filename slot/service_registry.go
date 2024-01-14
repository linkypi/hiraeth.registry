package slot

import (
	"strconv"
	"sync"
	"time"
)

const (
	InstanceIdPrefix       = "instance-"
	InstanceKeyPrefix      = "instance-key-"
	InstanceListenerPrefix = "instance-listener-"
)

type ServiceRegistry struct {
	sync.Map
	ServiceListener
}

type ServiceListener struct {
	connId string
}

func (s *ServiceListener) onFire(serviceName string, instances []ServiceInstance) {

}

func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{}
}

func (s *ServiceRegistry) getInstance(serviceName string) []ServiceInstance {
	instances, ok := s.Load(InstanceKeyPrefix + serviceName)
	if ok {
		return instances.([]ServiceInstance)
	}
	return nil
}

func (s *ServiceRegistry) Subscribe(serviceName string, connId string) {
	listeners, ok := s.Load(InstanceListenerPrefix + serviceName)
	if !ok {
		listeners = make(map[string][]ServiceListener)
	}
	list := listeners.([]ServiceListener)
	serviceListeners := append(list, ServiceListener{connId: connId})
	s.Store(InstanceListenerPrefix+serviceName, serviceListeners)
}

func (s *ServiceRegistry) Unsubscribe(serviceName string, connId string) {
	key := InstanceListenerPrefix + serviceName
	listeners, ok := s.Load(key)
	if ok {
		list := listeners.([]ServiceListener)
		for i, listener := range list {
			if listener.connId == connId {
				list = append(list[:i], list[i+1:]...)
				s.Store(key, list)
				break
			}
		}
	}
}

func (s *ServiceRegistry) Register(serviceName string, instanceIp string, instancePort int) {
	instance := ServiceInstance{
		serviceName:          serviceName,
		instanceIp:           instanceIp,
		instancePort:         instancePort,
		lastestHeartbeatTime: time.Now(),
	}

	instances, ok := s.Load(InstanceKeyPrefix + serviceName)
	if !ok {
		list := make([]ServiceInstance, 16)
		list = append(list, instance)
		s.Store(serviceName, list)
		return
	}

	var exist bool
	list := instances.([]ServiceInstance)
	for _, service := range list {
		if service.getInstanceId() == instance.getInstanceId() {
			exist = true
			service.lastestHeartbeatTime = time.Now()
		}
	}
	if !exist {
		list = append(list, instance)
		s.Store(InstanceIdPrefix+instance.getInstanceId(), instances)
		// Notifies the client that the subscription information has been updated
		go s.onFire(serviceName, list)
	}
	s.Store(InstanceKeyPrefix+serviceName, list)
}

type ServiceInstance struct {
	serviceName          string
	instanceIp           string
	instancePort         int
	lastestHeartbeatTime time.Time
}

func NewServiceInstance(serviceName string, instanceIp string, instancePort int) *ServiceInstance {
	return &ServiceInstance{
		serviceName:          serviceName,
		instanceIp:           instanceIp,
		instancePort:         instancePort,
		lastestHeartbeatTime: time.Now(),
	}
}

func (s *ServiceInstance) getInstanceId() string {
	return s.instanceIp + ":" + strconv.Itoa(s.instancePort)
}
