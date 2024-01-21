package common

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

type Message struct {
	MessageType MessageType `json:"messageType"`
	RequestType RequestType `json:"requestType"`
	RequestId   uint64      `json:"requestId"`
	Timestamp   int64       `json:"timestamp"`

	Payload []byte
}

var ErrorMetadataChanged = errors.New("metadata changed")

type RequestType uint8

func (t RequestType) String() string {
	switch t {
	case Nop:
		return "Nop"
	case Heartbeat:
		return "Heartbeat"
	case Register:
		return "Register"
	case Subscribe:
		return "Subscribe"
	case PublishServiceChanged:
		return "PublishServiceChanged"
	case FetchMetadata:
		return "FetchMetadata"
	case FetchServiceInstance:
		return "FetchServiceInstance"
	default:
		return ""
	}
}

const (
	Nop RequestType = iota
	Heartbeat
	Register
	Subscribe
	UnSubscribe
	PublishServiceChanged
	FetchMetadata
	FetchServiceInstance
)

type MessageType uint8

func (t MessageType) String() string {
	switch t {
	case RequestMsg:
		return "request"
	case ResponseMsg:
		return "response"
	default:
		return ""
	}
}

const (
	NoneMsg MessageType = iota
	RequestMsg
	ResponseMsg
)

type Request struct {
	Message
}
type Response struct {
	Message
	Success   bool
	Code      string
	Msg       string
	ErrorType uint8
}

func ParseMsgFrom(buffer []byte) Message {
	messageType := MessageType(buffer[0])
	requestType := RequestType(buffer[1])
	requestId := binary.BigEndian.Uint64(buffer[2:10])
	timestamp := binary.BigEndian.Uint64(buffer[10:18])
	return Message{
		MessageType: messageType,
		RequestType: requestType,
		RequestId:   requestId,
		Timestamp:   int64(timestamp),
		Payload:     buffer[18:]}
}

func NewRequest(requestType RequestType, payload []byte) Request {
	message := Message{
		MessageType: RequestMsg,
		RequestType: requestType,
		Payload:     payload,
		Timestamp:   time.Now().Unix(),
		RequestId:   uint64(GenerateId()),
	}
	return Request{message}
}

func NewResponse(requestId uint64, payload []byte) Response {
	message := Message{
		MessageType: ResponseMsg,
		Payload:     payload,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
	}
	return Response{Message: message}
}

func NewErrResponse(requestId uint64, code, msg string) Response {
	message := Message{
		MessageType: ResponseMsg,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
	}
	return Response{
		Message: message,
		Success: false,
		Code:    code,
		Msg:     msg,
	}
}

func NewOkResponse(requestId uint64, requestType RequestType) Response {
	message := Message{
		MessageType: ResponseMsg,
		RequestType: requestType,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
	}
	return Response{
		Message: message,
		Success: true,
	}
}

func NewOkResponseWithPayload(requestId uint64, requestType RequestType, payload []byte) Response {
	message := Message{
		MessageType: ResponseMsg,
		RequestType: requestType,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
		Payload:     payload,
	}
	return Response{
		Message: message,
		Success: true,
	}
}

func NewErrResponseWithMsg(requestId uint64, requestType RequestType, msg string, errType uint8) Response {
	message := Message{
		MessageType: ResponseMsg,
		RequestType: requestType,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
	}
	return Response{
		ErrorType: errType,
		Message:   message,
		Success:   false,
		Msg:       msg,
	}
}

func NewErrResponseWithPayload(requestId uint64, payload []byte) Response {
	message := Message{
		MessageType: ResponseMsg,
		Payload:     payload,
		Timestamp:   time.Now().Unix(),
		RequestId:   requestId,
	}
	return Response{Message: message, Success: false}
}

func (obj *Response) Encode() ([]byte, error) {
	return Encode(obj)
}

func (obj *Request) Encode() ([]byte, error) {
	return Encode(obj)
}

func (obj *Request) ToBytes() ([]byte, error) {
	byes, err := obj.Encode()
	if err != nil {
		return nil, err
	}
	byes, err = appendMsgTypeToHeader(RequestMsg, byes)
	if err != nil {
		return nil, err
	}
	return byes, nil
}

func (obj *Response) ToBytes() ([]byte, error) {
	byes, err := obj.Encode()
	if err != nil {
		return nil, err
	}
	byes, err = appendMsgTypeToHeader(ResponseMsg, byes)
	if err != nil {
		return nil, err
	}
	return byes, nil
}

func BuildRequestToBytes(requestType RequestType, payload proto.Message) (uint64, []byte, error) {
	byes, err := EncodePb(payload)
	if err != nil {
		return 0, nil, err
	}
	request := NewRequest(requestType, byes)
	byes, err = request.Encode()
	if err != nil {
		return 0, nil, err
	}
	byes, err = appendMsgTypeToHeader(RequestMsg, byes)
	if err != nil {
		return 0, nil, err
	}

	return request.RequestId, byes, nil
}

func BuildResponseToBytes(requestId uint64, payload proto.Message) ([]byte, error) {
	byes, err := EncodePb(payload)
	if err != nil {
		return nil, err
	}
	request := NewResponse(requestId, byes)
	byes, err = request.Encode()
	if err != nil {
		return nil, err
	}
	byes, err = appendMsgTypeToHeader(ResponseMsg, byes)
	if err != nil {
		return nil, err
	}
	return byes, nil
}

func appendMsgTypeToHeader(messageType MessageType, data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 1))
	err := binary.Write(buf, binary.BigEndian, uint8(messageType))
	if err != nil {
		return nil, err
	}
	data = append(buf.Bytes(), data...)
	return data, nil
}

func appendDataLenToHeader(bytes []byte) ([]byte, error) {
	lengthBytes, err := GetMessageLengthBytes(uint32(len(bytes)))
	if err != nil {
		return nil, err
	}
	bytes = append(lengthBytes, bytes...)
	return bytes, nil
}

func Decode(data []byte, obj any) error {
	buf := new(bytes.Buffer)
	buf.Write(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(obj); err != nil {
		return err
	}
	return nil
}

// Encode The efficiency of encoding and decoding with gob is low, and optimization will be considered later
func Encode(obj any) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		log.Fatal("encode error:", err)
	}
	return buf.Bytes(), nil
}

// ReadMessageLengthFromHeader Read the message length, which is usually fixed at 4 bytes
func ReadMessageLengthFromHeader(bytes []byte, len int) int {
	return int(binary.BigEndian.Uint32(bytes[0:len]))
}

func GetMessageLengthBytes(dateLen uint32) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, DecoderConfig.LengthFieldLength))
	err := binary.Write(buf, binary.BigEndian, dateLen)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeToRequest(buffer []byte) (*Request, error) {
	buf := bytes.NewBuffer(buffer)
	msg := &Request{}
	if err := binary.Read(buf, binary.BigEndian, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func DecodeToResponse(buffer []byte) (*Response, error) {
	buf := bytes.NewBuffer(buffer)
	msg := &Response{}
	if err := binary.Read(buf, binary.BigEndian, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func DecodeToPb(buffer []byte, pbm proto.Message) error {
	err := proto.Unmarshal(buffer, pbm)
	if err != nil {
		return err
	}
	return nil
}

func EncodePb(pbm proto.Message) ([]byte, error) {
	bys, err := proto.Marshal(pbm)
	if err != nil {
		return bys, err
	}
	return bys, nil
}
