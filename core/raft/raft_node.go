package raft

import (
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/linkypi/hiraeth.registry/config"
	core "github.com/linkypi/hiraeth.registry/core/network"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"os"
	"time"
)

type RaftNode struct {
	manager *core.NetworkManager
	log     logrus.Logger
}

func (rn *RaftNode) SetNetWorkManager(manager *core.NetworkManager) {
	rn.manager = manager
}

// transport returns a raft.Transport that communicates over gRPC.
func (rn *RaftNode) transport() raft.Transport {
	return raftAPI{rn.manager}
}

func RegisterRaftTransportService(grpcServer *grpc.Server, net *core.NetworkManager) {
	pb.RegisterRaftTransportServer(grpcServer, grpcAPI{manager: net})
}

func (rn *RaftNode) Start(nodeId, dataDir string, peers []raft.Server, clusterConfig config.ClusterConfig,
	notifyCh chan bool, fsm raft.FSM) (*raft.Raft, error) {

	conf := raft.DefaultConfig()
	conf.HeartbeatTimeout = time.Duration(clusterConfig.RaftHeartbeatTimeout) * time.Millisecond
	conf.ElectionTimeout = time.Duration(clusterConfig.RaftElectionTimeout) * time.Millisecond
	conf.NotifyCh = notifyCh
	conf.LocalID = raft.ServerID(nodeId)

	baseDir := dataDir + "/" + "raft"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		err = os.MkdirAll(baseDir, 0755)
		return nil, fmt.Errorf(`create raft data dir [%s] failed: %v`, baseDir, err)
	}

	logPath := baseDir + "/logs.dat"
	ldb, err := boltdb.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf(`boltdb create log store failed(%q): %v`, logPath, err)
	}

	stablePath := baseDir + "/stable.dat"
	sdb, err := boltdb.NewBoltStore(stablePath)
	if err != nil {
		return nil, fmt.Errorf(`boltdb create stable store failed(%q): %v`, stablePath, err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 20, rn.log.Out)
	if err != nil {
		return nil, fmt.Errorf(`raft create snapshot store failed(%q, ...): %v`, baseDir, err)
	}

	r, err := raft.NewRaft(conf, fsm, ldb, sdb, fss, rn.transport())
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	cfg := raft.Configuration{Servers: peers}

	// use boltDb to read the value of the key [CurrentTerm] to determine whether a cluster exists
	// If the cluster already exists, use raft.RecoverCluster() method to restore the cluster
	state, err := raft.HasExistingState(ldb, sdb, fss)
	if state && err == nil {
		rn.log.Info("raft cluster already exists, restore cluster instead.")
		err := raft.RecoverCluster(conf, fsm, ldb, sdb, fss, rn.transport(), cfg)
		if err != nil {
			return r, err
		}
		return r, nil
	}

	fur := r.BootstrapCluster(cfg)
	if err := fur.Error(); err != nil {
		return nil, fmt.Errorf("raft bootstrap cluster failed: %v", err)
	}

	return r, nil
}
