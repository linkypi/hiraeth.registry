package slot

import (
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/sirupsen/logrus"
)

type Bucket struct {
	*ServiceRegistry
	index     int
	nodeId    string
	isReplica bool
}

func newBucket(index int, nodeId string, isReplica bool, clusterConfig config.ClusterConfig, shutdownCh chan struct{}, log *logrus.Logger) *Bucket {
	return &Bucket{
		index:           index,
		nodeId:          nodeId,
		isReplica:       isReplica,
		ServiceRegistry: NewServiceRegistry(clusterConfig, shutdownCh, log),
	}
}
