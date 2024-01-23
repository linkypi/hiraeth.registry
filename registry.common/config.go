package common

import (
	"encoding/binary"
	pb "github.com/linkypi/hiraeth.registry/common/proto"
	"github.com/panjf2000/gnet"
)

const (
	SlotsCount = 16384
	RemoteAddr = "RemoteAddr"
)

var (
	EncoderConfig = gnet.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}
	DecoderConfig = gnet.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
)

func ConvertReqType(reqType RequestType) pb.RequestType {
	switch reqType {
	case Register:
		return pb.RequestType_Register
	case Heartbeat:
		return pb.RequestType_Heartbeat
	}
	return pb.RequestType_Unknown
}
