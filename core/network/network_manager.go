package network

import (
	"github.com/linkypi/hiraeth.registry/config"
	"github.com/sirupsen/logrus"
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
	log          *logrus.Logger
	LocalAddress raft.ServerAddress

	RpcChan          chan raft.RPC
	HeartbeatFunc    func(raft.RPC)
	HeartbeatFuncMtx sync.Mutex
	HeartbeatTimeout time.Duration

	ConnectionsMtx sync.Mutex
	Connections    map[string]*conn
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

func NewNetworkManager(localAddress string, log *logrus.Logger) *NetworkManager {
	address := raft.ServerAddress(localAddress)
	m := &NetworkManager{
		log:          log,
		LocalAddress: address,
		RpcChan:      make(chan raft.RPC),
		Connections:  map[string]*conn{},
	}
	return m
}

func (net *NetworkManager) GetConnectedNodes(clusterServers map[string]*config.NodeInfo) []config.NodeInfo {
	arr := make([]config.NodeInfo, 0, 8)
	for _, node := range clusterServers {
		_, ok := net.Connections[node.Id]
		if ok {
			arr = append(arr, *node)
		}
	}
	return arr
}

func (net *NetworkManager) GetRaftClient(id string) (pb.RaftTransportClient, error) {
	if !net.ExistConn(id) {
		return nil, errors.New("connection not exist, id: " + id)
	}
	return net.Connections[id].raftClient, nil
}

func (net *NetworkManager) GetInterRpcClient(id string) pb.ClusterServiceClient {
	if !net.ExistConn(id) {
		return nil
	}
	return net.Connections[id].internalClient
}

func (net *NetworkManager) IsConnected(id string) bool {
	if !net.ExistConn(id) {
		return false
	}
	con := net.Connections[id]
	state := con.grpcConn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (net *NetworkManager) ExistConn(id string) bool {
	_, ok := net.Connections[id]
	return ok
}

func (net *NetworkManager) AddConn(id string, grpcConn *grpc.ClientConn,
	internalServClient pb.ClusterServiceClient, raftClient pb.RaftTransportClient) {

	net.ConnectionsMtx.Lock()
	c, ok := net.Connections[id]
	if !ok {
		c = &conn{}
		c.mtx.Lock()
		net.Connections[id] = c
	}
	net.ConnectionsMtx.Unlock()
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

func (net *NetworkManager) CloseAllConn() error {
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
	}

	if err != errCloseErr {
		return err
	}

	return nil
}

func (net *NetworkManager) CloseConn(nodeId string) error {
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
	}
	delete(net.Connections, nodeId)
	return nil
}

func (net *NetworkManager) CheckConnClosed(shutDownCh chan struct{}, reconnect func(string)) {
	for {
		select {
		case <-shutDownCh:
			return
		default:
		}

		for id, con := range net.Connections {
			state := con.grpcConn.GetState()
			net.log.Debugf("Connection state for %s: %s", id, state.String())

			if state == connectivity.TransientFailure || state == connectivity.Shutdown {
				net.log.Warnf("Connection to node %s is closed or in transient failure state.", id)
				net.ConnectionsMtx.Lock()
				// Prevent goroutines from concurrently accessing disconnected connections
				con.mtx.Lock()
				err := con.grpcConn.Close()
				if err != nil {
					net.log.Errorf("Error closing connection: %s", err)
					continue
				}
				con.raftClient = nil
				con.internalClient = nil
				con.mtx.Unlock()
				delete(net.Connections, id)
				net.ConnectionsMtx.Unlock()

				// Re-establish the connection
				go reconnect(id)
			}

			time.Sleep(2 * time.Second)
		}
	}
}
