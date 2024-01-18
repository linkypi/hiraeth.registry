package common

import (
	"strconv"
	"time"
)

type ServiceInstanceState uint8

const (
	UnknownState ServiceInstanceState = iota
	Healthy
	UnHealthy
)

func (s *ServiceInstanceState) String() string {
	switch *s {
	case UnknownState:
		return "UnknownState"
	case Healthy:
		return "Healthy"
	case UnHealthy:
		return "UnHealthy"
	}
	return ""
}

type ServiceInstance struct {
	ServiceName       string `json:"serviceName"`
	InstanceIp        string `json:"instanceIp"`
	InstancePort      int    `json:"instancePort"`
	LastHeartbeatTime time.Time
	State             ServiceInstanceState
}

func (s *ServiceInstance) GetInstanceId() string {
	return s.ServiceName + "/" + s.InstanceIp + ":" + strconv.Itoa(s.InstancePort)
}
