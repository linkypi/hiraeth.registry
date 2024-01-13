package util

import (
	"errors"
	"github.com/sourcegraph/conc"
	"sync/atomic"
	"time"
)

var (
	ExecTimeoutErr = errors.New("exec timeout")
)

func MapToList[T any](list map[any]T) []T {
	var result = make([]T, 0, len(list))
	for _, node := range list {
		result = append(result, node)
	}
	return result
}

func WaitForAllExecDone[T any](arr []T, execFunc func(T) bool) (uint64, []T, bool) {

	if arr == nil || len(arr) == 0 {
		return 0, nil, false
	}
	var numOfAck uint64
	resultCh := make(chan T, len(arr))

	waitGroup := conc.NewWaitGroup()
	for _, e := range arr {
		n := e
		waitGroup.Go(func() {
			success := execFunc(n)
			if success {
				resultCh <- n
				atomic.AddUint64(&numOfAck, 1)
			}
		})
	}
	waitGroup.Wait()

	index := 0
	jumpOut := false
	results := make([]T, 0, int(numOfAck))
	for {
		select {
		case e := <-resultCh:
			index++
			results = append(results, e)
			if index == int(numOfAck) {
				jumpOut = true
				break
			}
		}
		if jumpOut {
			break
		}
	}

	if int(numOfAck) < len(arr) {
		return numOfAck, results, false
	}

	return numOfAck, results, true
}

func WaitUntilExecSuccess(timeout time.Duration, stopCh chan struct{}, execFunc func(...any) error) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-stopCh:
			return errors.New("received stop signal")
		case <-timer.C:
			return ExecTimeoutErr
		default:
		}

		err := execFunc()
		// try again
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		return nil
	}
}
