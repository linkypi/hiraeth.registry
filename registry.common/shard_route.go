package common

import (
	"errors"
	"github.com/spaolacci/murmur3"
)

func GetBucketIndex(serviceName string) int {
	hashCode := murmur3.Sum64([]byte(serviceName)) & 0x7fffffff
	slotIndex := int(hashCode) % SlotsCount
	return slotIndex
}

func GetNodeIdByIndex(index int, shards map[string]Shard) (string, error) {
	if index < 0 || index >= SlotsCount {
		return "", errors.New("index out of range")
	}

	if shards == nil || len(shards) == 0 {
		return "", errors.New("shards is nil or empty")
	}

	for nodeId, shard := range shards {
		for _, segment := range shard.Segments {
			if segment.Start <= index && segment.End >= index {
				return nodeId, nil
			}
		}
	}

	return "", errors.New("not found")
}
