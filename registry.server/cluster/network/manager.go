package network

import (
	"github.com/hashicorp/go-multierror"
	"github.com/linkypi/hiraeth.registry/server/config"
	"github.com/linkypi/hiraeth.registry/server/log"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/connectivity"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Manager struct {
	log          *logrus.Logger
	LocalAddress raft.ServerAddress

	RpcChan          chan raft.RPC
	HeartbeatFunc    func(raft.RPC)
	HeartbeatFuncMtx sync.Mutex
	HeartbeatTimeout time.Duration

	ConnectionsMtx sync.Mutex
	Connections    map[string]*conn

	// address -> nodeId
	AddrIdMap map[string]string
}

var (
	errCloseErr = errors.New("error closing connections")
)

type conn struct {
	Addr     string
	grpcConn *grpc.ClientConn
	// just for raft node communications
	raftClient *pb.RaftTransportClient
	// just for cluster node communications
	PeerClient *pb.ClusterServiceClient
	mtx        sync.Mutex
}

func NewNetworkManager(localAddress string) *Manager {
	address := raft.ServerAddress(localAddress)
	m := &Manager{
		log:          log.Log,
		LocalAddress: address,
		RpcChan:      make(chan raft.RPC),
		Connections:  map[string]*conn{},
		AddrIdMap:    make(map[string]string),
	}
	return m
}

func (net *Manager) GetConnectedNodeIds(clusterServers map[string]*config.NodeInfo) []string {
	arr := make([]string, 0, 8)
	for _, node := range clusterServers {
		_, ok := net.Connections[node.Id]
		if ok {
			arr = append(arr, node.Id)
		}
	}
	return arr
}

func (net *Manager) GetConnectedNodes(clusterServers map[string]config.NodeInfo) []config.NodeInfo {
	arr := make([]config.NodeInfo, 0, 8)
	for _, node := range clusterServers {
		_, ok := net.Connections[node.Id]
		if ok {
			arr = append(arr, node)
		}
	}
	return arr
}

func (net *Manager) GetRaftClient(id string) (*pb.RaftTransportClient, error) {
	if !net.ExistConn(id) {
		return nil, errors.New("connection not exist, id: " + id)
	}
	conn := net.Connections[id]

	defer func() {
		if err := recover(); err != nil {
			net.log.Errorf("panic when get raft client, id: %s, err: %v", id, err)
		}
	}()
	if conn.raftClient == nil {
		net.log.Warnf("raft connection to node %s is nil, rpc con: %s.", id, conn.grpcConn.Target())
	}
	return conn.raftClient, nil
}

func (net *Manager) GetInterRpcClient(id string) *pb.ClusterServiceClient {
	if !net.ExistConn(id) {
		return nil
	}
	return net.Connections[id].PeerClient
}

func (net *Manager) IsConnected(id string) bool {
	if !net.ExistConn(id) {
		return false
	}
	con := net.Connections[id]
	state := con.grpcConn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (net *Manager) ExistConn(id string) bool {
	_, ok := net.Connections[id]
	return ok
}

func (net *Manager) GetConnByAddr(addr string) (*conn, error) {
	addr = strings.Replace(addr, "localhost", "127.0.0.1", -1)
	id, ok := net.AddrIdMap[addr]
	if !ok {
		return &conn{}, errors.New("connection not exist, addr: " + addr)
	}
	con, ok := net.Connections[id]
	if !ok {
		return &conn{}, errors.New("connection not exist, addr: " + addr)
	}
	return con, nil
}

func (net *Manager) AddConn(id, addr string, grpcConn *grpc.ClientConn,
	internalServClient *pb.ClusterServiceClient, raftClient *pb.RaftTransportClient) {
	net.ConnectionsMtx.Lock()
	defer net.ConnectionsMtx.Unlock()

	net.AddrIdMap[addr] = id

	con, ok := net.Connections[id]
	if !ok {
		con = &conn{
			Addr:       addr,
			raftClient: raftClient,
			grpcConn:   grpcConn,
			PeerClient: internalServClient,
		}
		con.mtx.Lock()
		defer con.mtx.Unlock()
		net.Connections[id] = con
		net.log.Infof("connection to node %s is established.", id)
		return
	}

	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.Addr = addr
	con.grpcConn = grpcConn
	con.raftClient = raftClient
	con.PeerClient = internalServClient
	net.log.Infof("update connection to node %s.", id)

}

func (net *Manager) CloseAllConn() error {
	net.ConnectionsMtx.Lock()
	defer net.ConnectionsMtx.Unlock()

	err := errCloseErr
	for _, conn := range net.Connections {
		// Lock conn.mtx to ensure Dial() is complete
		conn.mtx.Lock()
		conn.mtx.Unlock()
		closeErr := conn.grpcConn.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
		delete(net.AddrIdMap, conn.Addr)
	}

	if err != errCloseErr {
		return err
	}

	return nil
}

func (net *Manager) CloseConn(nodeId string) error {
	net.ConnectionsMtx.Lock()
	defer net.ConnectionsMtx.Unlock()

	for id, conn := range net.Connections {
		if id != nodeId {
			continue
		}
		conn.mtx.Lock()
		conn.mtx.Unlock()
		closeErr := conn.grpcConn.Close()
		if closeErr != nil {
			return closeErr
		}
		delete(net.AddrIdMap, conn.Addr)
	}
	delete(net.Connections, nodeId)

	return nil
}

func (net *Manager) CheckConnClosed(shutDownCh chan struct{}, reconnect func(string)) {
	for {
		select {
		case <-shutDownCh:
			return
		default:
		}

		for id, con := range net.Connections {
			if con.grpcConn == nil {
				continue
			}
			state := con.grpcConn.GetState()
			net.log.Debugf("Connection state for %s: %s", id, state.String())

			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				net.log.Warnf("Connection to node %s is closed or in transient failure state.", id)
				//net.ConnectionsMtx.Lock()
				//// Prevent goroutines from concurrently accessing disconnected connections
				//con.mtx.Lock()
				//err := con.grpcConn.Close()
				//if err != nil {
				//	net.log.Errorf("Error closing connection: %s", err)
				//	continue
				//}
				//con.raftClient = nil
				//con.PeerClient = nil
				//con.mtx.Unlock()
				//delete(net.Connections, id)
				//delete(net.AddrIdMap, con.Addr)
				//net.ConnectionsMtx.Unlock()

				// Re-establish the connection
				go reconnect(id)
			}

			time.Sleep(5 * time.Second)
		}
	}
}
