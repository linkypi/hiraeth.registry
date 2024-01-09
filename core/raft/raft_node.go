package raft

import (
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	core "github.com/linkypi/hiraeth.registry/core/network"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"path/filepath"
)

type RaftNode struct {
	manager *core.NetworkManager
	log     logrus.Logger
}

func (rn *RaftNode) SetNetWorkManager(manager *core.NetworkManager) {
	rn.manager = manager
}

// transport returns a raft.Transport that communicates over gRPC.
func (rn *RaftNode) transport(grpcRegister grpc.ServiceRegistrar) raft.Transport {
	pb.RegisterRaftTransportServer(grpcRegister, gRPCAPI{manager: rn.manager})
	return raftAPI{rn.manager}
}

func (rn *RaftNode) StartRaftNode(nodeId, logDir string, peers []raft.Server,
	grpcRegister grpc.ServiceRegistrar, notifyCh chan bool, fsm raft.FSM) (*raft.Raft, error) {

	conf := raft.DefaultConfig()
	conf.NotifyCh = notifyCh
	conf.LocalID = raft.ServerID(nodeId)
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

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, rn.log.Out)
	if err != nil {
		return nil, fmt.Errorf(`raft create snapshot store failed(%q, ...): %v`, baseDir, err)
	}

	r, err := raft.NewRaft(conf, fsm, ldb, sdb, fss, rn.transport(grpcRegister))
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	cfg := raft.Configuration{
		Servers: peers,
		//	[]raft.Server{
		//	{
		//		Suffrage: raft.Voter,
		//		ID:       raft.ServerID(nodeId),
		//		Address:  raft.ServerAddress(nodeAddr),
		//	},
		//},
	}
	f := r.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return nil, fmt.Errorf("raft bootstrap cluster failed: %v", err)
	}

	return r, nil
}
