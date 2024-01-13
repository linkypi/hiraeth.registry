package test

import (
	"encoding/json"
	"fmt"
	"github.com/linkypi/hiraeth.registry/core/slot"
	"strconv"
	"testing"
)

func TestSlotAllocation(t *testing.T) {
	numOfServer := 5
	numOfReplicas := 5
	nodes := make([]string, 0, numOfServer)
	for i := 1; i <= numOfServer; i++ {
		node := strconv.Itoa(i)
		nodes = append(nodes, node)
	}

	slotsAllocation := slot.ExecuteSlotsAllocation("1", nodes)
	replicasAllocation := slot.ExecuteSlotReplicasAllocation(slotsAllocation, nodes, numOfReplicas)

	slots, _ := json.Marshal(slotsAllocation)
	fmt.Println(string(slots))
	replicas, _ := json.Marshal(replicasAllocation)
	fmt.Println(string(replicas))

}
