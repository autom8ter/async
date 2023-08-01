package async

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Borrower is a thread-safe object that can be borrowed and returned.
type Borrower[T any] struct {
	v      atomic.Pointer[T]
	ch     chan *T
	closed *bool
	once   sync.Once
	mu     sync.Mutex
}

// NewBorrower returns a new Borrower with the provided value.
func NewBorrower[T any](value T) *Borrower[T] {
	closed := false
	b := &Borrower[T]{
		ch:     make(chan *T, 1),
		v:      atomic.Pointer[T]{},
		closed: &closed,
	}
	b.v.Store(&value)
	b.ch <- b.v.Load()
	return b
}

// Borrow returns the value of the Borrower. If the value is not available, it will block until it is.
func (b *Borrower[T]) Borrow() *T {
	return <-b.ch
}

// TryBorrow returns the value of the Borrower if it is available. If the value is not available, it will return false.
func (b *Borrower[T]) TryBorrow() (*T, bool) {
	select {
	case value, ok := <-b.ch:
		if !ok {
			return nil, false
		}
		return value, true
	default:
		return nil, false
	}
}

// BorrowContext returns the value of the Borrower. If the value is not available, it will block until it is or the context is canceled.
func (b *Borrower[T]) BorrowContext(ctx context.Context) (*T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	select {
	case value, ok := <-b.ch:
		if !ok {
			return nil, fmt.Errorf("borrower closed")
		}
		return value, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Return returns the value to the Borrower so it can be borrowed again.
// If the value is not a pointer to the value that was borrowed, it will return an error.
// If the value has already been returned, it will return an error.
func (b *Borrower[T]) Return(obj *T) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.v.Load() != obj {
		return fmt.Errorf("object returned to borrower is not the same as the object that was borrowed")
	}
	if len(b.ch) > 0 {
		return fmt.Errorf("object already returned to borrower")
	}
	b.v.Store(obj)
	b.ch <- obj
	return nil
}

// Value returns the value of the Borrower. This is a non-blocking operation since the value is not borrowed(non-pointer).
func (b *Borrower[T]) Value() T {
	return *b.v.Load()
}

// Do borrows the value, calls the provided function, and returns the value.
func (b *Borrower[T]) Do(fn func(*T)) error {
	value := b.Borrow()
	fn(value)
	return b.Return(value)
}

// Swap borrows the value, swaps it with the provided value, and returns the value to the Borrower.
func (b *Borrower[T]) Swap(value T) error {
	_, ok := b.TryBorrow()
	if !ok {
		return fmt.Errorf("borrower closed")
	}
	b.v.Swap(&value)
	return b.Return(&value)
}

// Close closes the Borrower and prevents it from being borrowed again. If the Borrower is still borrowed, it will return an error.
// Close is idempotent.
func (b *Borrower[T]) Close() error {
	var err error
	b.once.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if len(b.ch) > 0 {
			err = fmt.Errorf("value is still borrowed")
			return
		}
		if !*b.closed {
			close(b.ch)
			*b.closed = true
			b.v = atomic.Pointer[T]{}
		}
	})
	return err
}
