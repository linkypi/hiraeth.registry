package test

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestReturn(t *testing.T) {
	res, err := bar()
	fmt.Println(res, err)
}

func TestTime(t *testing.T) {
	currentTime := time.Now()
	formatString := "2006-01-02 15:04:05.000"
	formattedTime := currentTime.Format(formatString)
	fmt.Println(formattedTime)
}

type MyType struct {
	x int
}

func bar() (res *MyType, err error) {
	if 1 == 1 {
		myType := MyType{x: 112}
		res, err = &myType, errors.New("term not match")
		return &myType, errors.New("xxx")
	}

	defer func() {
		if e := recover(); e != nil {

			err = errors.New(fmt.Sprintf("%v", e))
		}
	}()
	return &MyType{x: 654}, nil
}
