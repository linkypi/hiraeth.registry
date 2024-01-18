package common

import (
	"context"
	"google.golang.org/appengine/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"net"
	"time"
)

type GrpcServer struct {
	Server *grpc.Server
	Socket net.Listener
}

var defaultEnforcementPolicy = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var defaultKeepaliveServerParameters = keepalive.ServerParameters{
	// If a client is idle for 15 seconds, send a GOAWAY
	MaxConnectionIdle: 15 * time.Second,
	// If any connection is alive for more than 30 seconds, send a GOAWAY
	MaxConnectionAge: 30 * time.Second,
	// Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	MaxConnectionAgeGrace: 5 * time.Second,
	// Ping the client if it is idle for 5 seconds to ensure the connection is still active
	Time: 5 * time.Second,
	// Wait 1 second for the ping ack before assuming the connection is dead
	Timeout: 1 * time.Second,
}

func StartGRPCServer(addr string, shutDownCh chan struct{}, serverCh chan *GrpcServer, register func(*grpc.Server)) {
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorf(context.Background(), "failed to listen: %v", err)
		close(shutDownCh)
		return
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(defaultEnforcementPolicy),
		grpc.KeepaliveParams(defaultKeepaliveServerParameters))
	reflection.Register(grpcServer)
	register(grpcServer)
	// notify if grpcServer has been created
	serverCh <- &GrpcServer{
		Server: grpcServer,
		Socket: sock,
	}

	// start grpc server，enter an infinite loop after the startup is complete
	if err := grpcServer.Serve(sock); err != nil {
		log.Errorf(context.Background(), "grpc server failed to serve: %v", err)
		close(shutDownCh)
	}
}

func StartGRPCServerWithParameters(addr string, knp keepalive.EnforcementPolicy, ksp keepalive.ServerParameters,
	shutDownCh chan struct{}, serverCh chan *GrpcServer) {
	sock, err := net.Listen("tcp", addr)
	if err != nil {
		log.Errorf(context.Background(), "failed to listen: %v", err)
		close(shutDownCh)
		return
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(knp), grpc.KeepaliveParams(ksp))
	reflection.Register(grpcServer)

	// notify if grpcServer has been created
	serverCh <- &GrpcServer{
		Server: grpcServer,
		Socket: sock,
	}

	// start grpc server，enter an infinite loop after the startup is complete
	if err := grpcServer.Serve(sock); err != nil {
		log.Errorf(context.Background(), "grpc server failed to serve: %v", err)
		close(shutDownCh)
	}
}
