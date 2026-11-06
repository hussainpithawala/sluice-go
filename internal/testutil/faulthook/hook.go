package faulthook

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrInjected = errors.New("faulthook: injected error")

type faultKind int

const (
	kindFailBefore faultKind = iota
	kindFailAfter
	kindDelay
)

type fault struct {
	kind      faultKind
	remaining int32
	delay     time.Duration
}

type Hook struct {
	mu     sync.Mutex
	faults map[string]*fault // scriptSHA or command name → fault
}

func New() *Hook {
	return &Hook{faults: make(map[string]*fault)}
}

// FailBefore causes the next `count` executions of commands matching `pattern`
// to return ErrInjected BEFORE the command reaches Redis.
func (h *Hook) FailBefore(pattern string, count int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faults[strings.ToLower(pattern)] = &fault{kind: kindFailBefore, remaining: int32(count)}
}

// FailAfter causes the next `count` executions of commands matching `pattern`
// to return a connection-reset error AFTER the command is sent to Redis (ambiguous failure).
func (h *Hook) FailAfter(pattern string, count int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faults[strings.ToLower(pattern)] = &fault{kind: kindFailAfter, remaining: int32(count)}
}

// Delay adds latency to the next `count` executions matching `pattern`.
func (h *Hook) Delay(pattern string, d time.Duration, count int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faults[strings.ToLower(pattern)] = &fault{kind: kindDelay, remaining: int32(count), delay: d}
}

// Reset removes all configured faults.
func (h *Hook) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faults = make(map[string]*fault)
}

func (h *Hook) match(cmdName string, args []interface{}) *fault {
	h.mu.Lock()
	defer h.mu.Unlock()

	name := strings.ToLower(cmdName)

	// Direct command match
	if f, ok := h.faults[name]; ok && atomic.LoadInt32(&f.remaining) > 0 {
		return f
	}

	// For EVALSHA, check if any fault pattern matches the SHA or args
	if name == "evalsha" && len(args) > 0 {
		sha := strings.ToLower(argString(args[0]))
		if f, ok := h.faults[sha]; ok && atomic.LoadInt32(&f.remaining) > 0 {
			return f
		}
	}

	return nil
}

func (h *Hook) consume(f *fault) bool {
	return atomic.AddInt32(&f.remaining, -1) >= 0
}

func (h *Hook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *Hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		f := h.match(cmd.Name(), cmd.Args())
		if f != nil && h.consume(f) {
			switch f.kind {
			case kindFailBefore:
				cmd.SetErr(ErrInjected)
				return ErrInjected
			case kindDelay:
				time.Sleep(f.delay)
			case kindFailAfter:
				err := next(ctx, cmd)
				if err != nil {
					return err
				}
				connErr := &net.OpError{Op: "read", Err: errors.New("connection reset by peer (injected)")}
				cmd.SetErr(connErr)
				return connErr
			}
		}
		return next(ctx, cmd)
	}
}

func (h *Hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			f := h.match(cmd.Name(), cmd.Args())
			if f != nil && h.consume(f) {
				switch f.kind {
				case kindFailBefore:
					for _, c := range cmds {
						c.SetErr(ErrInjected)
					}
					return ErrInjected
				case kindDelay:
					time.Sleep(f.delay)
				case kindFailAfter:
					err := next(ctx, cmds)
					if err != nil {
						return err
					}
					connErr := &net.OpError{Op: "read", Err: errors.New("connection reset by peer (injected)")}
					for _, c := range cmds {
						c.SetErr(connErr)
					}
					return connErr
				}
			}
		}
		return next(ctx, cmds)
	}
}

func argString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
