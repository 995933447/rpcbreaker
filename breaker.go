package rpcbreaker

import (
	"sync"
	"sync/atomic"
	"time"
)

var DisabledMatchByRpcService bool

type RpcBreaker interface {
	OnRpc(req []interface{}, resp []interface{})
	Fallback(...interface{}) []interface{}
	Break() bool
}

var breakers sync.Map

func RegisterRpcBreaker(rpcSvc, rpcMethod string, breaker RpcBreaker) {
	breakers.Store(genBreakerKey(rpcSvc, rpcMethod), breaker)
}

func GetRpcBreaker(rpcSvc, rpcMethod string) (RpcBreaker, bool) {
	breaker, ok := breakers.Load(genBreakerKey(rpcSvc, rpcMethod))
	if !ok {
		return nil, false
	}
	if !DisabledMatchByRpcService {
		breaker, ok = breakers.Load(genBreakerKey(rpcSvc, "*"))
		if !ok {
			return nil, false
		}
	}
	b, ok := breaker.(RpcBreaker)
	if !ok {
		return nil, false
	}
	return b, true
}

func genBreakerKey(rpcSvc, rpcMethod string) string {
	return rpcSvc + "@" + rpcMethod
}

func DoWithBreaker(rpcSvc, rpcMethod string, doRpc func() []interface{}, req ...interface{}) []interface{} {
	breaker, ok := GetRpcBreaker(rpcSvc, rpcMethod)
	if !ok {
		return doRpc()
	}
	if breaker.Break() {
		return breaker.Fallback(req...)
	}
	resp := doRpc()
	breaker.OnRpc(req, resp)
	return resp
}

type FailCountThreshold struct {
	failSpreed                         sync.Map
	failTotal                          atomic.Int64
	consecutiveFailSec, thresholdCount int64
	mu                                 sync.RWMutex
}

func NewFailCountThreshold(consecutiveSec int64, thresholdCount int64) *FailCountThreshold {
	s := &FailCountThreshold{
		consecutiveFailSec: consecutiveSec,
		thresholdCount:     thresholdCount,
	}
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		lastRmExpiredAt := time.Now().Unix()
		for {
			<-tk.C
			now := time.Now().Unix()
			expired := now - consecutiveSec
			s.mu.RLock()
			for i := lastRmExpiredAt; i <= expired; i++ {
				if count, ok := s.failSpreed.Load(expired); ok {
					s.failSpreed.Delete(expired)
					s.failTotal.Add(-count.(*atomic.Int64).Load())
				}
			}
			s.mu.RUnlock()
			lastRmExpiredAt = now
		}
	}()
	return s
}

func (s *FailCountThreshold) AddFail() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count, _ := s.failSpreed.LoadOrStore(time.Now().Unix(), &atomic.Int64{})
	count.(*atomic.Int64).Add(1)
	s.failTotal.Add(1)
}

func (s *FailCountThreshold) Reached() bool {
	return s.failTotal.Load() >= s.thresholdCount
}

func (s *FailCountThreshold) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSpreed = sync.Map{}
	s.failTotal.Store(0)
}

type FailCountThresholdBreaker struct {
	threshold         *FailCountThreshold
	halfOpenThreshold *FailCountThreshold
	halfOpenTimeLong  time.Duration
	breakTime         time.Time
	breakTimeLong     time.Duration
	isRpcFail         func(req []interface{}, resp []interface{}) bool
	fallback          func(...interface{}) []interface{}
}

func NewFailCountThresholdBreaker(
	timeWinSec, thresholdCount, halfOpenTimeWinSec, halfOpenThresholdCount int64,
	breakTimeLong time.Duration,
	isRpcFail func(req []interface{}, resp []interface{}) bool,
	fallback func(...interface{}) []interface{},
) *FailCountThresholdBreaker {
	return &FailCountThresholdBreaker{
		threshold:         NewFailCountThreshold(timeWinSec, thresholdCount),
		halfOpenThreshold: NewFailCountThreshold(halfOpenTimeWinSec, halfOpenThresholdCount),
		halfOpenTimeLong:  time.Duration(halfOpenTimeWinSec) * time.Second,
		breakTimeLong:     breakTimeLong,
		isRpcFail:         isRpcFail,
		fallback:          fallback,
	}
}

func (f *FailCountThresholdBreaker) OnRpc(req []interface{}, resp []interface{}) {
	if !f.isRpcFail(req, resp) {
		return
	}
	if f.isHalfOpen() {
		f.halfOpenThreshold.AddFail()
		if f.halfOpenThreshold.Reached() {
			f.doBreak()
		}
		return
	}
	f.threshold.AddFail()
	if f.threshold.Reached() {
		f.doBreak()
	}
}

func (f *FailCountThresholdBreaker) doBreak() {
	f.breakTime = time.Now()
	f.threshold.Clear()
	f.halfOpenThreshold.Clear()
}

func (f *FailCountThresholdBreaker) isHalfOpen() bool {
	return time.Since(f.breakTime.Add(f.breakTimeLong)) <= f.halfOpenTimeLong
}

func (f *FailCountThresholdBreaker) Fallback(req ...interface{}) []interface{} {
	return f.fallback(req...)
}

func (f *FailCountThresholdBreaker) Break() bool {
	return time.Now().Sub(f.breakTime) < f.breakTimeLong
}
