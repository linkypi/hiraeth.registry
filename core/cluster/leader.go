package cluster

import (
	"errors"
	"github.com/hashicorp/raft"
	"github.com/linkypi/hiraeth.registry/config"
	"time"
)

type Leader struct {
	*BaseCluster
	id   string
	addr string
	term int
}

func NewLeader(term int, id, addr string) *Leader {
	return &Leader{id: id, term: term, addr: addr}
}

func (l *Leader) AddNewNode(remoteNode *config.NodeInfo) error {

	if l.Raft == nil {
		l.Log.Errorf("[cluster] raft is nil, can not add new node, node id: %s, node addr: %s", remoteNode.Id, remoteNode.Addr)
		return errors.New("raft is nil, can not add new node")
	}
	// Check whether the connection of the new node exists, and if not, establish the connection first
	if !l.ExistConn(remoteNode.Id) {
		l.ConnectToNode(*remoteNode)
	}
	if l.State != Active {
		l.Log.Errorf("[cluster] cluster state is not active: %s, can not add new node, id: %s, addr: %s",
			l.State.String(), remoteNode.Id, remoteNode.Addr)
		return errors.New("cluster state is not active")
	}
	// only the leader node has the permission to add nodes, otherwise the addition fails
	if !l.IsLeader() {
		return errors.New("only the leader node has the permission to add nodes")
	}
	var indexFur raft.IndexFuture
	if remoteNode.IsCandidate {
		indexFur = l.Raft.AddVoter(
			raft.ServerID(remoteNode.Id),
			raft.ServerAddress(remoteNode.Addr),
			0,
			3*time.Second,
		)
	} else {
		indexFur = l.Raft.AddNonvoter(
			raft.ServerID(remoteNode.Id),
			raft.ServerAddress(remoteNode.Addr),
			0,
			3*time.Second,
		)
	}

	err := indexFur.Error()
	if err != nil {
		// add later , configuration changed since 30 (latest is 1)
		l.Log.Errorf("[cluster] add new node to cluster failed, node id: %s, addr: %s. %v", remoteNode.Id, remoteNode.Addr, err)
		return err
	}

	l.Log.Infof("[cluster] add new node to cluster success, node id: %s, addr: %s", remoteNode.Id, remoteNode.Addr)
	return nil

}
