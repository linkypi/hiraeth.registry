package raft

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/linkypi/hiraeth.registry/server/cluster/network"
	"github.com/linkypi/hiraeth.registry/server/config"
	"google.golang.org/grpc"
	"os"
	"time"
)

type RaftNode struct {
	net *network.Manager
	*raft.Raft
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
	notifyCh chan bool, fsm raft.FSM) error {

	conf := raft.DefaultConfig()
	conf.NotifyCh = notifyCh
	conf.LogOutput = common.GetLogger().Out
	conf.LocalID = raft.ServerID(nodeId)
	conf.ProtocolVersion = raft.ProtocolVersionMax
	conf.LogLevel = clusterConfig.LogLevel.String()
	conf.HeartbeatTimeout = time.Duration(clusterConfig.RaftHeartbeatTimeout) * time.Millisecond
	conf.ElectionTimeout = time.Duration(clusterConfig.RaftElectionTimeout) * time.Millisecond

	loggerWrapper := common.LoggerWrapper{Logger: common.GetLogger()}
	loggerWrapper.SetName("raft")
	conf.Logger = &loggerWrapper

	// create raft data dir
	baseDir := dataDir + "/" + "raft"
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		err = os.MkdirAll(baseDir, 0644)
		if err != nil {
			return fmt.Errorf(`create raft data dir [%s] failed: %v`, baseDir, err)
		}
	}

	logPath := baseDir + "/logs.db"
	ldb, err := boltdb.NewBoltStore(logPath)
	if err != nil {
		return fmt.Errorf(`boltdb create log store failed(%q): %v`, logPath, err)
	}

	stablePath := baseDir + "/stable.db"
	sdb, err := boltdb.NewBoltStore(stablePath)
	if err != nil {
		return fmt.Errorf(`boltdb create stable store failed(%q): %v`, stablePath, err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 20, common.GetLogger().Out)
	if err != nil {
		return fmt.Errorf(`raft create snapshot store failed(%q, ...): %v`, baseDir, err)
	}

	r, err := raft.NewRaft(conf, fsm, ldb, sdb, fss, rn.transport())
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %v", err)
	}

	rn.Raft = r
	// 有可能 raft 集群已经存在, 此时需要判断原有集群节点与当前已经连接的节点是否同在一个集群
	currentCfg := raft.Configuration{Servers: peers}

	// use boltDb to read the value of the key [CurrentTerm] to determine whether a cluster exists
	// If the cluster state not exists, use r.BootstrapCluster() method to create a new one
	existState, err := raft.HasExistingState(ldb, sdb, fss)
	if err != nil {
		return fmt.Errorf("raft check existing state failed: %v", err)
	}
	if !existState {
		fur := r.BootstrapCluster(currentCfg)
		if err := fur.Error(); err != nil {
			return fmt.Errorf("raft bootstrap cluster failed: %v", err)
		}
		return nil
	}

	currentTerm, err := sdb.GetUint64([]byte("CurrentTerm"))
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}

	//err = rn.checkLeaderTerm(sdb, ldb)
	//if err != nil {
	//	return err
	//}

	common.Infof("raft cluster already exists, current term: %d, check the configuration", currentTerm)
	// 新增节点配置对比检查
	oldCfgFuture := r.GetConfiguration()
	if err := oldCfgFuture.Error(); err != nil {
		return fmt.Errorf("failed to get current config: %v", err)
	}
	oldCfg := oldCfgFuture.Configuration()

	// 新增详细节点对比逻辑
	added, removed, changed := diffNodes(oldCfg.Servers, currentCfg.Servers)

	// 记录节点差异详情

	if len(removed) > 0 {
		common.Warnf("removed nodes in configuration: %v", logNodes(removed))
		//for _, server := range removed {
		//	err := r.RemoveServer(server.ID, 0, time.Second*10).Error()
		//	if err != nil {
		//		common.Errorf("failed to remove server %s: %s", server.ID, server.Address, err)
		//	}
		//}
	}
	if len(changed) > 0 {
		common.Errorf("changed nodes in configuration: %v", logChangedNodes(changed, currentCfg.Servers))
		return errors.New("cluster node has changed")
		//for _, server := range changed {
		//	err := r.RemoveServer(server.ID, 0, time.Second*10).Error()
		//	if err != nil && err.Error() != "ignore" {
		//		common.Errorf("failed to remove server %s: %s", server.ID, server.Address, err)
		//	}
		//}
		//
		//// 找到新id对的节点
		//for _, server := range changed {
		//	for _, newServer := range currentCfg.Servers {
		//		if newServer.ID == server.ID {
		//			err := r.AddVoter(newServer.ID, newServer.Address, 0, time.Second*10).Error()
		//			if err != nil && err.Error() != "ignore" {
		//				common.Errorf("failed to add server %s: %s", newServer.ID, newServer.Address, err)
		//				return err
		//			}
		//			break
		//		}
		//	}
		//}
	}
	if len(added) > 0 {
		//for _, server := range added {
		//	err := rn.addNode(server)
		//	if err != nil {
		//		common.Errorf("failed to remove server %s: %s, err: %v", server.ID, server.Address, err)
		//		return err
		//	}
		//}
		common.Warnf("new nodes in configuration: %v", logNodes(added))
	}
	// 计算总差异节点数
	//totalDiff := len(added) + len(removed) + len(changed)
	//if totalDiff > 0 {
	//	common.Warnf("cluster configuration changed: %d nodes affected", totalDiff)
	//}
	// 计算节点差异率
	//diffRatio := calculateConfigDiff(currentCfg.Servers, cfg.Servers)
	//if diffRatio > 0.5 { // 超过50%节点变化则报错
	//	return fmt.Errorf("node configuration changed too much (%.0f%% nodes changed), "+
	//		"manual intervention required", diffRatio*100)
	//}

	return nil
}
func (rn *RaftNode) addNode(node raft.Server) error {
	server := raft.Server{
		ID:       node.ID,
		Address:  node.Address,
		Suffrage: raft.Voter,
	}

	// 使用正确的参数顺序和配置索引
	err := rn.Raft.AddVoter(
		server.ID,
		server.Address,
		0,              // 使用实际的最新配置索引
		10*time.Second, // 添加合理的超时时间
	).Error()
	if err != nil {
		return fmt.Errorf("failed to add voter, id: %v, addr: %v, err: %v", server.ID, server.Address, err)
	}
	return nil
}
func isSameConfiguration(a, b raft.Configuration) bool {
	if len(a.Servers) != len(b.Servers) {
		return false
	}

	aMap := make(map[raft.ServerID]raft.Server)
	for _, s := range a.Servers {
		aMap[s.ID] = s
	}

	for _, s := range b.Servers {
		aServer, ok := aMap[s.ID]
		if !ok {
			return false
		}
		if aServer.Address != s.Address || aServer.Suffrage != s.Suffrage {
			return false
		}
	}
	return true
}

func (rn *RaftNode) checkLeaderTerm(sdb *boltdb.BoltStore, ldb *boltdb.BoltStore) error {
	// 从稳定存储获取 CurrentTerm
	currentTerm, err := sdb.GetUint64([]byte("CurrentTerm"))
	if err != nil {
		return fmt.Errorf("failed to get current term: %v", err)
	}

	// 从日志存储获取最后日志条目
	lastLogIndex, _ := ldb.LastIndex()
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLog := &raft.Log{}
		err := ldb.GetLog(lastLogIndex, lastLog)
		if err != nil || lastLog == nil {
			return fmt.Errorf("failed to get last log term: %v", err)
		}
		lastLogTerm = lastLog.Term
	}

	// 检查 Term 一致性
	if currentTerm != lastLogTerm {
		return fmt.Errorf("term inconsistency (current:%d vs lastLog:%d)",
			currentTerm, lastLogTerm)
	}
	return nil
}
func diffNodes(old, new []raft.Server) (added, removed, changed []raft.Server) {
	oldMap := make(map[raft.ServerID]raft.Server)
	for _, s := range old {
		oldMap[s.ID] = s
	}

	newMap := make(map[raft.ServerID]raft.Server)
	for _, s := range new {
		newMap[s.ID] = s
		if _, exists := oldMap[s.ID]; !exists {
			added = append(added, s)
		}
	}

	for _, s := range old {
		if _, exists := newMap[s.ID]; !exists {
			removed = append(removed, s)
		} else if newMap[s.ID].Address != s.Address {
			changed = append(changed, s)
		}
	}
	return
}
func logNodes(servers []raft.Server) []string {
	var res []string
	for _, s := range servers {
		res = append(res, fmt.Sprintf("[ID:%s Addr:%s]", s.ID, s.Address))
	}
	return res
}

func logChangedNodes(oldServers, newServers []raft.Server) []string {
	var res []string
	for _, s := range oldServers {
		for _, server := range newServers {
			if s.ID == server.ID {
				res = append(res, fmt.Sprintf("[ID:%s OldAddr:%s NewAddr:%s]",
					s.ID, s.Address, server.Address))
			}
		}
	}
	return res
}
func calculateConfigDiff(old, new []raft.Server) float64 {
	oldMap := make(map[raft.ServerID]bool)
	for _, s := range old {
		oldMap[s.ID] = true
	}

	changed := 0
	for _, s := range new {
		if !oldMap[s.ID] {
			changed++
		}
	}
	return float64(changed) / float64(len(new))
}

//func (rn *RaftNode) RecoverRaftCluster(protocolVersion int, peersFile string) error {
//	common.Info("found peers.json file, recovering Raft configuration...")
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
//	common.Info("deleted peers.json file after successful recovery")
//	return nil
//}

func (rn *RaftNode) AddNode(nodeId, nodeAddr string, suffrage raft.ServerSuffrage, prevLogIndex uint64) error {
	if rn.Raft == nil {
		return fmt.Errorf("raft instance is nil")
	}

	server := raft.Server{
		ID:       raft.ServerID(nodeId),
		Address:  raft.ServerAddress(nodeAddr),
		Suffrage: suffrage,
	}

	future := rn.Raft.AddVoter(server.ID, server.Address, prevLogIndex, time.Second*10)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add node %s: %v", nodeId, err)
	}

	return nil
}

func (rn *RaftNode) RemoveNode(nodeId string) error {
	if rn.Raft == nil {
		return fmt.Errorf("raft instance is nil")
	}

	future := rn.Raft.RemoveServer(raft.ServerID(nodeId), 0, time.Second*10)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove node %s: %v", nodeId, err)
	}

	return nil
}
