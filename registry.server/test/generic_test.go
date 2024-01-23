package test

import (
	"fmt"
	"github.com/linkypi/hiraeth.registry/common"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestGeneric(r *testing.T) {
	request := pb.RegisterRequest{
		ServiceName: "service1", ServiceIp: "127.0.0.1", ServicePort: 8080,
	}

	bytes, _ := common.EncodePb(&request)

	//req := &pb.RegisterRequest{}
	//result, err := decode(bytes, req)
	//req = result.(*pb.RegisterRequest)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}
	//fmt.Println(req.ServiceName, req.ServiceIp, req.ServicePort)

	req := &pb.RegisterRequest{}
	err := decode2(bytes, req)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(req.ServiceName, req.ServiceIp, req.ServicePort)
}

func decode[T proto.Message](payload []byte, request T) (any, error) {
	err := proto.Unmarshal(payload, request)
	if err != nil {
		return nil, err
	}
	return request, err
}
func decode2[T proto.Message](payload []byte, request T) error {
	err := proto.Unmarshal(payload, request)
	if err != nil {
		return err
	}
	return nil
}
