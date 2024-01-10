package test

import "testing"

import (
	"context"
	pb "github.com/linkypi/hiraeth.registry/proto"
	"google.golang.org/grpc"
	"log"
)

// First, start the localhost:2665 and localhost:2666 nodes to build a new cluster
// Then run the following test case to see if the cluster is successfully joined with localhost:2664
// The actual result can be successfully joined, but it will be prompted "leadership lost while committing log"
func TestJoinCluster(t *testing.T) {
	request := pb.JoinClusterRequest{
		NodeId:                "1",
		NodeAddr:              "localhost:2664",
		AutoJoinClusterEnable: true,
		IsCandidate:           true,
	}
	sendJoinRequest("localhost:2665", &request)
	sendJoinRequest("localhost:2666", &request)
}

func sendJoinRequest(remoteAddr string, req *pb.JoinClusterRequest) {
	conn, err := grpc.Dial(remoteAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)

	// Make a request to the server
	response, err := client.JoinCluster(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to call JoinCluster: %v", err)
	}

	log.Printf("Response from server: %v", response)
	log.Println("join cluster success")
}
