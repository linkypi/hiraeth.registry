package test

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	pb "github.com/linkypi/hiraeth.registry/server/proto"
	"testing"
)

func TestMessageMarshal(t *testing.T) {
	//_ = common.InitSnowFlake("", 1)
	//var generatedMessage proto.GeneratedMessage
	//v2 := proto.MessageV2(generatedMessage)
	response := pb.RegisterResponse{Success: true}
	//message := common.Message{
	//	Message:     v2,
	//	MessageType: common.RequestMsg,
	//	RequestType: common.Register,
	//	RequestId:   uint64(common.GenerateId()),
	//}
	//bytes, err := proto.Marshal(&response)
	msgBytes, err := proto.Marshal(&response)
	if err != nil {
		t.Error(err)
	}
	newMsg := pb.RegisterResponse{}
	err = proto.Unmarshal(msgBytes, &newMsg)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(newMsg)
}
