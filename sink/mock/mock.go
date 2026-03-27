// Package mock provides a thread-safe in-memory FlushSink for tests.
package mock

import (
	"context"
	"sync"

	"github.com/hussainpithawala/sluice-go/sink"
)

// Sink is a thread-safe in-memory FlushSink for testing.
type Sink struct {
	mu          sync.RWMutex
	written     map[string]sink.WriteModel
	bulkCalls   int
	singleCalls int
	closed      bool
	pingErr     error
	bulkErr     error
}

func New() *Sink { return &Sink{written: make(map[string]sink.WriteModel)} }

func (m *Sink) WithPingError(err error) *Sink { m.pingErr = err; return m }
func (m *Sink) WithBulkError(err error) *Sink { m.bulkErr = err; return m }

func (m *Sink) BulkWrite(_ context.Context, models []sink.WriteModel) (*sink.BulkWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bulkErr != nil {
		return nil, m.bulkErr
	}
	m.bulkCalls++
	for _, model := range models {
		m.written[model.CorrelationKey] = model
	}
	return &sink.BulkWriteResult{UpsertedCount: int64(len(models))}, nil
}

func (m *Sink) Write(_ context.Context, model sink.WriteModel) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.singleCalls++
	m.written[model.CorrelationKey] = model
	return nil
}

func (m *Sink) Ping(_ context.Context) error { return m.pingErr }
func (m *Sink) Close(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *Sink) Written() map[string]sink.WriteModel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]sink.WriteModel, len(m.written))
	for k, v := range m.written {
		out[k] = v
	}
	return out
}
func (m *Sink) WrittenCount() int    { m.mu.RLock(); defer m.mu.RUnlock(); return len(m.written) }
func (m *Sink) BulkCallCount() int   { m.mu.RLock(); defer m.mu.RUnlock(); return m.bulkCalls }
func (m *Sink) SingleCallCount() int { m.mu.RLock(); defer m.mu.RUnlock(); return m.singleCalls }
func (m *Sink) IsClosed() bool       { m.mu.RLock(); defer m.mu.RUnlock(); return m.closed }
func (m *Sink) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written = make(map[string]sink.WriteModel)
	m.bulkCalls, m.singleCalls = 0, 0
}
