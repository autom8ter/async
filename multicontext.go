package async

import (
	"context"
	"sync"
)

// MultiContext is a context that can be used to combine contexts with a root context so they can be cancelled together.
type MultiContext struct {
	context.Context
	mu      sync.Mutex
	cancel  context.CancelFunc
	cancels []context.CancelFunc
}

// NewMultiContext returns a new MultiContext.
func NewMultiContext(ctx context.Context) *MultiContext {
	ctx, cancel := context.WithCancel(ctx)
	m := &MultiContext{
		Context: ctx,
		cancel:  cancel,
	}
	go func() {
		select {
		case <-m.Done():
			m.mu.Lock()
			for _, cancel := range m.cancels {
				cancel()
			}
			m.mu.Unlock()
		}
	}()
	return m
}

// WithCloser adds a function to be called when the multi context is cancelled.
func (m *MultiContext) WithCloser(fn func()) {
	m.cancels = append(m.cancels, fn)
}

// WithContext returns a new context that is a child of the root context.
// This context will be cancelled when the multi context is cancelled.
func (m *MultiContext) WithContext(ctx context.Context) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	m.cancels = append(m.cancels, cancel)
	return ctx
}

// Cancel cancels all child contexts.
func (m *MultiContext) Cancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cancel := range m.cancels {
		cancel()
	}
	m.cancel()
}
