package test

import (
	"context"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"google.golang.org/grpc"
	"log"
	"testing"
)

func TestGrpc(t *testing.T) {
	request := pb.TransferRequest{
		Term:     123,
		LeaderId: "12",
		Addr:     "",
		Status:   pb.TransferStatus_Transitioning,
	}
	conn, err := grpc.Dial("127.0.0.1:2666", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewClusterServiceClient(conn)

	response, err := client.TransferLeadership(context.Background(), &request)
	log.Println(response, err)
}
