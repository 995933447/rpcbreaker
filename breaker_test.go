package rpcbreaker

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailCountThreshold(t *testing.T) {
	s := NewFailCountThreshold(10, 12)
	tk := time.NewTicker(time.Second)
	now := time.Now()
	for range tk.C {
		s.AddFail()
		s.AddFail()
		if s.Reached() {
			t.Errorf("Reached fail count, over sec:%d", time.Since(now)/time.Second)
			return
		}
	}
}

type TestFailCountThresholdBreakerReq struct {
}

type TestFailCountThresholdBreakerResp struct {
}

func TestFailCountThresholdBreaker(t *testing.T) {
	RegisterRpcBreaker("local", "test", NewFailCountThresholdBreaker(
		60, 10,
		10, 5,
		time.Second*3,
		func(req []interface{}, resp []interface{}) bool {
			if len(resp) >= 2 && resp[1].(error) != nil {
				return true
			}
			return false
		},
		func(req ...interface{}) []interface{} {
			fmt.Println("fallback")
			return []interface{}{&TestFailCountThresholdBreakerResp{}, nil}
		},
	))

	id := atomic.Int32{}
	for i := 0; i < 30; i++ {
		id.Add(1)
		DoWithBreaker("local", "test", func() []interface{} {
			fmt.Println("local.test", id.Load())
			return []interface{}{nil, errors.New("test error")}
		}, &TestFailCountThresholdBreakerReq{})
		time.Sleep(time.Second)
	}
}
