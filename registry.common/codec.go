package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet"
	gerr "github.com/panjf2000/gnet/pkg/errors"
)

type ICodec interface {
	gnet.ICodec

	// DecodeFor for windows
	DecodeFor(data []byte) ([]byte, error)
	// EncodeFor for windows
	EncodeFor(item ICodec, buf []byte) ([]byte, error)
}
type BuildInFixedLengthCodec struct {
	Version uint16
}

const (
	DefaultHeadLength      = 6 // uint16 + uint32
	DefaultProtocolVersion = 0x22
)

func (b *BuildInFixedLengthCodec) EncodeFor(codec ICodec, buf []byte) ([]byte, error) {
	targetCodec := codec.(*BuildInFixedLengthCodec)
	return b.encode(*targetCodec, buf)
}
func (b *BuildInFixedLengthCodec) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	// take out the param
	ctx := c.Context()
	codec := ctx.(*BuildInFixedLengthCodec)
	return b.encode(*codec, buf)
}

func (b *BuildInFixedLengthCodec) encode(item BuildInFixedLengthCodec, buf []byte) ([]byte, error) {
	result := make([]byte, 0)
	buffer := bytes.NewBuffer(result)
	// write protocol version
	if err := binary.Write(buffer, binary.BigEndian, item.Version); err != nil {
		s := fmt.Sprintf("Pack version error , %v", err)
		return nil, errors.New(s)
	}

	// write data length
	dataLen := uint32(len(buf))
	if err := binary.Write(buffer, binary.BigEndian, dataLen); err != nil {
		s := fmt.Sprintf("Pack datalength error , %v", err)
		return nil, errors.New(s)
	}

	if dataLen > 0 {
		// write the real data
		if err := binary.Write(buffer, binary.BigEndian, buf); err != nil {
			s := fmt.Sprintf("Pack data error , %v", err)
			return nil, errors.New(s)
		}
	}

	return buffer.Bytes(), nil
}

func (b *BuildInFixedLengthCodec) DecodeFor(data []byte) ([]byte, error) {
	// parse header
	headerLen := DefaultHeadLength // uint16 + uint32
	size := len(data)
	if size < headerLen {
		log.Warnf("not enough header data len: %d", size)
		return nil, gerr.ErrIncompletePacket
	}

	header := data[0:headerLen]
	byteBuffer := bytes.NewBuffer(header)
	var pbVersion uint16
	var dataLength uint32
	_ = binary.Read(byteBuffer, binary.BigEndian, &pbVersion)
	_ = binary.Read(byteBuffer, binary.BigEndian, &dataLength)

	// to check the protocol version, reset buffer if the version  is not correct
	if dataLength < 1 {
		log.Warnf("not enough data len: %d", dataLength)
		return nil, errors.New("not normal protocol")
	}

	if pbVersion != DefaultProtocolVersion {
		log.Warnf("The protocol version do not match: %d, should be %d", pbVersion, DefaultProtocolVersion)
		return nil, errors.New("not normal protocol")
	}

	// parse payload
	dataLen := int(dataLength) // max int32 can contain 210MB payload
	readLen := len(data[headerLen:])
	if readLen == dataLen {
		// return the payload of the data
		return data[headerLen:], nil
	}
	log.Warnf("not enough payload data, the actual number of bytes read is %d, should be: %d", readLen, dataLen)
	return nil, gerr.ErrIncompletePacket
}

func (b *BuildInFixedLengthCodec) Decode(c gnet.Conn) ([]byte, error) {
	// parse header
	headerLen := DefaultHeadLength // uint16 + uint32

	size, header := c.ReadN(headerLen)
	if size == 0 {
		return nil, gerr.ErrIncompletePacket
	}
	if size < headerLen {
		log.Warnf("not enough header data len: %d", size)
		return nil, gerr.ErrIncompletePacket
	}

	byteBuffer := bytes.NewBuffer(header)
	var pbVersion uint16
	var dataLength uint32
	_ = binary.Read(byteBuffer, binary.BigEndian, &pbVersion)
	_ = binary.Read(byteBuffer, binary.BigEndian, &dataLength)

	// to check the protocol version, reset buffer if the version  is not correct
	if dataLength < 1 {
		return nil, nil
	}

	if pbVersion != DefaultProtocolVersion {
		c.ResetBuffer()
		log.Warnf("The protocol version do not match: %d, should be %d", pbVersion, DefaultProtocolVersion)
		return nil, errors.New("not normal protocol")
	}

	// parse payload
	dataLen := int(dataLength) // max int32 can contain 210MB payload
	protocolLen := headerLen + dataLen
	if dataSize, data := c.ReadN(protocolLen); dataSize == protocolLen {
		c.ShiftN(protocolLen)
		// return the payload of the data
		return data[headerLen:], nil
	}
	log.Warnf("not enough payload data, the actual number of bytes read is %d, should be: %d", dataLen, protocolLen)
	return nil, gerr.ErrIncompletePacket
}
