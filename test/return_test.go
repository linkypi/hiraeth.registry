package test

import (
	"errors"
	"fmt"
	"testing"
)

func TestReturn(t *testing.T) {
	res, err := bar()
	fmt.Println(res, err)
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
