package network

import (
	"github.com/linkypi/hiraeth.registry/config"
	"google.golang.org/grpc/connectivity"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type NetworkManager struct {
	LocalAddress raft.ServerAddress

	RpcChan          chan raft.RPC
	HeartbeatFunc    func(raft.RPC)
	HeartbeatFuncMtx sync.Mutex
	HeartbeatTimeout time.Duration

	ConnectionsMtx sync.Mutex
	Connections    map[raft.ServerID]*conn
}

var (
	errCloseErr = errors.New("error closing connections")
)

type conn struct {
	grpcConn *grpc.ClientConn
	// just for raft NodeInfo communications
	raftClient pb.RaftTransportClient
	// just for cluster server NodeInfo communications
	internalClient pb.ClusterServiceClient
	mtx            sync.Mutex
}

func (manager *NetworkManager) GetConnectedNodes(clusterServers map[string]*config.NodeInfo) []config.NodeInfo {
	arr := make([]config.NodeInfo, 0, 8)
	for _, node := range clusterServers {
		_, ok := manager.Connections[raft.ServerID(node.Id)]
		if ok {
			arr = append(arr, *node)
		}
	}
	return arr
}

func (manager *NetworkManager) GetRaftClient(id string) (pb.RaftTransportClient, error) {
	serverID := raft.ServerID(id)
	con, ok := manager.Connections[serverID]
	if ok {
		return con.raftClient, nil
	}
	return nil, errors.New(string("connection not exist, id: " + id))
}

func (manager *NetworkManager) GetInternalClient(id string) pb.ClusterServiceClient {
	serverID := raft.ServerID(id)
	return manager.Connections[serverID].internalClient
}

func (manager *NetworkManager) IsConnected(id string) bool {
	serverID := raft.ServerID(id)
	grpcConn := manager.Connections[serverID].grpcConn
	state := grpcConn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (manager *NetworkManager) AddConn(id string, grpcConn *grpc.ClientConn,
	internalServClient pb.ClusterServiceClient, raftClient pb.RaftTransportClient) {

	serverID := raft.ServerID(id)

	manager.ConnectionsMtx.Lock()
	c, ok := manager.Connections[serverID]
	if !ok {
		c = &conn{}
		c.mtx.Lock()
		manager.Connections[serverID] = c
	}
	manager.ConnectionsMtx.Unlock()
	if ok {
		c.mtx.Lock()
	}
	defer c.mtx.Unlock()
	if grpcConn != nil {
		c.grpcConn = grpcConn
	}
	if internalServClient != nil {
		c.internalClient = internalServClient
	}
	if raftClient != nil {
		c.raftClient = raftClient
	}
}

func NewNetworkManager(localAddress string, options ...Option) *NetworkManager {
	address := raft.ServerAddress(localAddress)
	m := &NetworkManager{
		LocalAddress: address,
		RpcChan:      make(chan raft.RPC),
		Connections:  map[raft.ServerID]*conn{},
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

func (manager *NetworkManager) Close() error {
	manager.ConnectionsMtx.Lock()
	defer manager.ConnectionsMtx.Unlock()

	err := errCloseErr
	for _, conn := range manager.Connections {
		// Lock conn.mtx to ensure Dial() is complete
		conn.mtx.Lock()
		conn.mtx.Unlock()
		closeErr := conn.grpcConn.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}

	if err != errCloseErr {
		return err
	}

	return nil
}
