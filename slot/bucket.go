package slot

type bucket struct {
	ServiceRegistry
	index     int
	nodeId    string
	isReplica bool
}

func newBucket(index int, nodeId string, isReplica bool) *bucket {
	return &bucket{index: index, nodeId: nodeId, isReplica: isReplica}
}
