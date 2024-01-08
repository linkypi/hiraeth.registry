package raft

import (
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	core "github.com/linkypi/hiraeth.registry/core"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
)

type RaftNode struct {
	manager *core.NetworkManager
}

func (rn *RaftNode) SetNetWorkManager(manager *core.NetworkManager) {
	rn.manager = manager
}

// Register the RaftTransport gRPC service on a gRPC server.
func (rn *RaftNode) register(s grpc.ServiceRegistrar) {
	pb.RegisterRaftTransportServer(s, gRPCAPI{manager: rn.manager})
}

// transport returns a raft.Transport that communicates over gRPC.
func (rn *RaftNode) transport(grpcRegister grpc.ServiceRegistrar) raft.Transport {
	rn.register(grpcRegister)
	return raftAPI{rn.manager}
}

func (rn *RaftNode) StartRaftNode(nodeId, nodeAddr, logDir string, grpcRegister grpc.ServiceRegistrar, fsm raft.FSM) (*raft.Raft, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(nodeId)
	baseDir := filepath.Join(logDir, "raft")

	logPath := filepath.Join(baseDir, "logs.dat")
	ldb, err := boltdb.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf(`boltdb create log store failed(%q): %v`, logPath, err)
	}

	stablePath := filepath.Join(baseDir, "stable.dat")
	sdb, err := boltdb.NewBoltStore(stablePath)
	if err != nil {
		return nil, fmt.Errorf(`boltdb create stable store failed(%q): %v`, stablePath, err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf(`raft create snapshot store failed(%q, ...): %v`, baseDir, err)
	}

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, rn.transport(grpcRegister))
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(nodeId),
				Address:  raft.ServerAddress(nodeAddr),
			},
		},
	}
	f := r.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return nil, fmt.Errorf("raft bootstrap cluster failed: %v", err)
	}

	return r, nil
}
