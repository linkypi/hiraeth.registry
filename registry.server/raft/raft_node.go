package raft

import (
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"os"
	"time"
)

type RaftNode struct {
	net *network.Manager
	log logrus.Logger
}

func (rn *RaftNode) SetNetWorkManager(manager *network.Manager) {
	rn.net = manager
}

// transport returns a raft.Transport that communicates over gRPC.
func (rn *RaftNode) transport() raft.Transport {
	return raftAPI{rn.net}
}

func RegisterRaftTransportService(grpcServer *grpc.Server, net *network.Manager) {
	pb.RegisterRaftTransportServer(grpcServer, grpcAPI{net: net})
}

func (rn *RaftNode) Start(nodeId, dataDir string, peers []raft.Server, clusterConfig config.ClusterConfig,
	notifyCh chan bool, fsm raft.FSM) (*raft.Raft, error) {

	conf := raft.DefaultConfig()
	conf.NotifyCh = notifyCh
	conf.LogOutput = rn.log.Out
	conf.LocalID = raft.ServerID(nodeId)
	conf.ProtocolVersion = raft.ProtocolVersionMax
	conf.LogLevel = clusterConfig.LogLevel.String()
	conf.HeartbeatTimeout = time.Duration(clusterConfig.RaftHeartbeatTimeout) * time.Millisecond
	conf.ElectionTimeout = time.Duration(clusterConfig.RaftElectionTimeout) * time.Millisecond

	baseDir := dataDir + "/" + "raft"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		err = os.MkdirAll(baseDir, 0644)
		if err != nil {
			return nil, fmt.Errorf(`create raft data dir [%s] failed: %v`, baseDir, err)
		}
	}

	logPath := baseDir + "/logs.db"
	ldb, err := boltdb.NewBoltStore(logPath)
	if err != nil {
		return nil, fmt.Errorf(`boltdb create log store failed(%q): %v`, logPath, err)
	}

	stablePath := baseDir + "/stable.db"
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

	// 有可能 raft 集群已经存在, 此时需要判断原有集群节点与当前已经连接的节点是否同在一个集群
	cfg := raft.Configuration{Servers: peers}

	// use boltDb to read the value of the key [CurrentTerm] to determine whether a cluster exists
	// If the cluster state not exists, use r.BootstrapCluster() method to create a new one
	existState, err := raft.HasExistingState(ldb, sdb, fss)
	if err != nil {
		return nil, fmt.Errorf("raft check existing state failed: %v", err)
	}
	if !existState {
		fur := r.BootstrapCluster(cfg)
		if err := fur.Error(); err != nil {
			return nil, fmt.Errorf("raft bootstrap cluster failed: %v", err)
		}
	}

	return r, nil
}

//func (rn *RaftNode) RecoverRaftCluster(protocolVersion int, peersFile string) error {
//	rn.log.Info("found peers.json file, recovering Raft configuration...")
//
//	var configuration raft.Configuration
//	var err error
//	if protocolVersion < 3 {
//		configuration, err = raft.ReadPeersJSON(peersFile)
//	} else {
//		configuration, err = raft.ReadConfigJSON(peersFile)
//	}
//	if err != nil {
//		return fmt.Errorf("recovery failed to parse peers.json: %v", err)
//	}
//
//	tmpFsm := &raftFsm{}
//	if err := raft.RecoverCluster(s.config.RaftConfig, tmpFsm,
//		log, stable, snap, trans, configuration); err != nil {
//		return fmt.Errorf("recovery failed: %v", err)
//	}
//
//	if err := os.Remove(peersFile); err != nil {
//		return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
//	}
//	rn.log.Info("deleted peers.json file after successful recovery")
//	return nil
//}

func (rn *RaftNode) AddNode(raftInstance *raft.Raft, nodeId, nodeAddr string, suffrage raft.ServerSuffrage, prevLogIndex uint64) error {
	if raftInstance == nil {
		return fmt.Errorf("raft instance is nil")
	}

	server := raft.Server{
		ID:       raft.ServerID(nodeId),
		Address:  raft.ServerAddress(nodeAddr),
		Suffrage: suffrage,
	}

	future := raftInstance.AddVoter(server.ID, server.Address, prevLogIndex, time.Second*10)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add node %s: %v", nodeId, err)
	}

	return nil
}

func (rn *RaftNode) RemoveNode(raftInstance *raft.Raft, nodeId string) error {
	if raftInstance == nil {
		return fmt.Errorf("raft instance is nil")
	}

	future := raftInstance.RemoveServer(raft.ServerID(nodeId), 0, time.Second*10)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove node %s: %v", nodeId, err)
	}

	return nil
}
