package client

import (
	pb "github.com/linkypi/hiraeth.registry/client/proto"
	"github.com/linkypi/hiraeth.registry/common"
	"github.com/sirupsen/logrus"
	"net"
	"testing"
	"time"
)

func TestAsyncRegisterUsingTcp(t *testing.T) {
	shutdownCh := make(chan struct{})
	client := initClient(t, "localhost:22662", shutdownCh)
	registerAsync("my-service", "127.0.0.1", 7380, client)
	select {
	case <-shutdownCh:
		if client != nil {
			client.Close()
		}
		return
	}
}

func TestAsyncRegisterUsingTcp2(t *testing.T) {
	shutdownCh := make(chan struct{})
	client := initClient(t, "localhost:22662", shutdownCh)
	registerAsync("my-service", "127.0.0.1", 9090, client)
	select {
	case <-shutdownCh:
		if client != nil {
			client.Close()
		}
		return
	}
}

func TestSubscribe(t *testing.T) {
	shutdownCh := make(chan struct{})
	client := initClient(t, "localhost:22662", shutdownCh)
	res, err := client.Subscribe("my-service", time.Second*10)
	if err != nil {
		t.Errorf("subscribe error: %v", err)
		close(shutdownCh)
		return
	}

	if res.Success {
		t.Logf("subscribe success")
		client.Close()
		return
	}

	if res.ErrorType == uint8(pb.ErrorType_MetaDataChanged.Number()) {

	}

	t.Logf("subscribe failed")
	client.Close()
}

func registerAsync(serviceName, ip string, port int, client *Client) {
	logger := common.Log
	_ = client.RegisterAsync(serviceName, ip, port, func(res common.Response, err2 error) {
		if err2 != nil {
			logger.Errorf("failed to register service: %v", err2)
			return
		}
		if res.Success {
			logger.Infof("register service success.")
			go client.sendHeartbeatsInPeriod()
		} else {
			logger.Errorf("failed to register service: %v", res.Msg)
		}
	})
}

func initClient(t *testing.T, serverAddr string, shutdownCh chan struct{}) *Client {
	common.InitLogger("./log", logrus.DebugLevel)
	_ = common.InitSnowFlake("", 1)
	logger := common.Log

	client, err := createClient(serverAddr, shutdownCh, logger)
	if err != nil {
		close(shutdownCh)
		t.Fatal(err)
	}

	defer func() {
		if e := recover(); e != nil {
			logger.Errorf("panic recovered: %v", e)
			close(shutdownCh)
		}
	}()
	return client
}

func TestSyncRegisterUsingTcp(t *testing.T) {
	shutdownCh := make(chan struct{})
	client := initClient(t, "localhost:22662", shutdownCh)
	res, err := client.Register("my-service", "127.0.0.1", 2345, time.Second*30)
	if err != nil {
		close(shutdownCh)
		t.Fatal("failed to register service", err)
		return
	}
	if !res.Success {
		t.Log("register failed: " + res.Msg)
		return
	}

	response := getRegResponse(t, err, res, shutdownCh)
	if response != nil {
		return
	}
	if response.Success {
		t.Log("register success")
	} else {
		t.Log("register failed: " + response.GetErrorType().String())
	}
}

func getRegResponse(t *testing.T, err error, res common.Response, shutdownCh chan struct{}) *pb.RegisterResponse {
	var resData pb.RegisterResponse
	err = common.DecodeToPb(res.Payload, &resData)
	if err != nil {
		close(shutdownCh)
		t.Fatal("failed to decode response", err)
		return nil
	}
	return &resData
}

func createClient(addr string, shutdownCh chan struct{}, logger *logrus.Logger) (*Client, error) {
	client := NewClient(4096, shutdownCh, logger)
	client.SetReadCallBack(func(bytes []byte, conn net.Conn, err error) {
		if err != nil {
			client.onReceive(nil, err)
			return
		}
		client.onReceive(bytes, nil)
	})
	err := client.Start(addr)
	if err != nil {
		client.Close()
		return nil, nil
	}
	return client, nil
}
