package client

import (
	pb "github.com/linkypi/hiraeth.registry/client/proto"
	"github.com/linkypi/hiraeth.registry/common"
	"testing"
	"time"
)

func TestAsyncRegisterUsingTcp(t *testing.T) {

	//termCh := make(chan os.Signal, 1)
	//signal.Notify(termCh, os.Interrupt, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGTERM)
	//go func() {
	//	<-termCh
	//	common.Log.Errorf("app closed with signal")
	//	os.Exit(0)
	//}()
	shutdownCh := make(chan struct{})
	client := initClient("127.0.0.1:22662", shutdownCh)
	registerAsync("my-service", "127.0.0.1", 7380, client)
	select {
	case <-shutdownCh:
		if client != nil {
			client.Close()
		}
		return
	}
}

func TestAsyncRegisterWithoutRoute(t *testing.T) {
	shutdownCh := make(chan struct{})
	addr := "127.0.0.1:22663"
	client := initClient(addr, shutdownCh)
	res, err := client.RegisterWithoutRoute("my-service", addr, "127.0.0.1", 7380, time.Second*60)
	if err != nil {
		t.Errorf("register error: %v", err)
		client.Close()
		return
	}
	if res.Success {
		t.Log("register success without route")
	} else {
		t.Errorf("register failed without route, %v", res.Msg)
	}
	select {
	case <-shutdownCh:
		if client != nil {
			client.Close()
		}
		return
	}
}

func TestAsyncRegisterUsingTcp3(t *testing.T) {
	shutdownCh := make(chan struct{})
	client := initClient("127.0.0.1:22663", shutdownCh)
	registerAsync("my-service", "127.0.0.1", 2346, client)
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
	client := initClient("127.0.0.1:22663", shutdownCh)
	registerAsync("my-service", "127.0.0.1", 9567, client)
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
	client := initClient("127.0.0.1:22662", shutdownCh)

	tryTimes := 0
	for {
		if tryTimes > 10 {
			return
		}
		res, err := client.Subscribe("my-service", time.Second*10)
		if err == nil {
			client.log.Info("subscribe success")
			break
		}
		tryTimes++
		client.log.Warnf("subscribe failed, will try again: %v", err)
		if res.ErrorType == uint8(pb.ErrorType_MetaDataChanged.Number()) {
			client.waitForFetchedMetadata()
		}
		continue
	}
	for {
		select {
		case <-client.shutdownCh:
			client.Close()
			return
		default:
		}
	}

}

func subscribe(client *Client, service string) bool {
	res, err := client.Subscribe(service, time.Second*10)
	if err != nil {
		return false
	}

	return res.Success
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

func initClient(serverAddr string, shutdownCh chan struct{}) *Client {

	logger := common.Log
	client, err := CreateClient(serverAddr, shutdownCh, logger)
	if err != nil {
		close(shutdownCh)
		logger.Errorf("failed to create client: %s", err)
		panic(err)
	}

	client.waitForFetchedMetadata()

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
	client := initClient("127.0.0.1:22662", shutdownCh)
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
