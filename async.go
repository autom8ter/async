/*
Package async is a package for asynchronous programming in Go.

It provides a set of primitives for safely building concurrent applications that don't leak resources or deadlock.
*/
package async

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ChannelGroup is a thread-safe group of channels. It is useful for broadcasting a value to multiple channels at once.
type ChannelGroup[T any] struct {
	ctx         *MultiContext
	subscribers sync.Map
	count       *int64
}

// NewChannelGroup returns a new ChannelGroup. The context is used to cancel all subscribers when the context is canceled.
// A channel group is useful for broadcasting a value to multiple subscribers.
func NewChannelGroup[T any](ctx context.Context) *ChannelGroup[T] {
	count := int64(0)
	return &ChannelGroup[T]{
		ctx:         NewMultiContext(ctx),
		subscribers: sync.Map{},
		count:       &count,
	}
}

// SendAsync sends a value to all channels in the group asynchronously. This is a non-blocking operation.
func (cg *ChannelGroup[T]) SendAsync(ctx context.Context, val T) chan bool {
	var channels []chan bool
	var ch = make(chan bool, 1)
	cg.subscribers.Range(func(key, state any) bool {
		c := state.(*Channel[T])
		channels = append(channels, c.SendAsync(ctx, val))
		return true
	})
	go func() {
		for _, ch := range channels {
			<-ch
		}
		ch <- true
	}()
	return ch
}

// SendAsync sends a value to all channels in the group asynchronously. This is a non-blocking operation.
func (cg *ChannelGroup[T]) Send(ctx context.Context, val T) {
	cg.subscribers.Range(func(key, state any) bool {
		c := state.(*Channel[T])
		c.Send(ctx, val)
		return true
	})
	return
}

// Channel returns a channel that will receive values from broadcasted values. The channel will be closed when the context is canceled.
// This is a non-blocking operation.
func (b *ChannelGroup[T]) Channel(ctx context.Context, opts ...ChannelOpt[T]) *ChannelReceiver[T] {
	ctx = b.ctx.WithContext(ctx)
	atomic.AddInt64(b.count, 1)
	id := fmt.Sprintf("%v-%v", *b.count, time.Now().UnixNano())
	opts = append(opts, WithOnClose[T](func(ctx context.Context) {
		b.subscribers.Delete(id)
		atomic.AddInt64(b.count, -1)
	}))
	ch := NewChannel(ctx, opts...)
	b.subscribers.Store(id, ch)
	return ch.ChannelReceiver
}

// Len returns the number of subscribers.
func (c *ChannelGroup[T]) Len() int {
	var count = 0
	c.subscribers.Range(func(key, state any) bool {
		count++
		return true
	})
	return count
}

// Close blocks until all subscribers have been removed and then closes the broadcast.
func (b *ChannelGroup[T]) Close() {
	b.ctx.Cancel()
	for b.Len() > 0 {
		time.Sleep(time.Millisecond * 10)
	}
}

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

// debugF logs a message if the DAGGER_DEBUG environment variable is set.
// It adds a stacktrace to the log message.
func debugF(format string, a ...interface{}) {
	if os.Getenv("ASYNC_DEBUG") != "" {
		format = fmt.Sprintf("DEBUG: %s\n", format)
		log.Printf(format, a...)
	}
}

// ChannelReceiver is a receiver for a channel.
type ChannelReceiver[T any] struct {
	ctx       *MultiContext
	ch        chan T
	closed    *int64
	wg        sync.WaitGroup
	onRcv     []func(context.Context, T) T
	where     []func(context.Context, T) bool
	closeOnce sync.Once
	onClose   []func(context.Context)
}

// Channel is a safer version of a channel that can be closed and has a context to prevent sending or receiving when the context is canceled.
type Channel[T any] struct {
	onSend []func(context.Context, T) T
	*ChannelReceiver[T]
}

type channelOpts[T any] struct {
	bufferSize int
	onSend     []func(context.Context, T) T
	onRcv      []func(context.Context, T) T
	onClose    []func(context.Context)
	where      []func(context.Context, T) bool
}

// ChannelOpt is an option for creating a new Channel.
type ChannelOpt[T any] func(*channelOpts[T])

// WithBufferSize sets the buffer size of the channel.
func WithBufferSize[T any](bufferSize int) ChannelOpt[T] {
	return func(opts *channelOpts[T]) {
		opts.bufferSize = bufferSize
	}
}

// WithOnSend adds a function to be called before sending a value.
func WithOnSend[T any](fn func(context.Context, T) T) ChannelOpt[T] {
	return func(opts *channelOpts[T]) {
		opts.onSend = append(opts.onSend, fn)
	}
}

// WithOnRcv adds a function to be called before receiving a value.
func WithOnRcv[T any](fn func(context.Context, T) T) ChannelOpt[T] {
	return func(opts *channelOpts[T]) {
		opts.onRcv = append(opts.onRcv, fn)
	}
}

// WithWhere adds a function to be called before sending a value to determine if the value should be sent.
func WithWhere[T any](fn func(context.Context, T) bool) ChannelOpt[T] {
	return func(opts *channelOpts[T]) {
		opts.where = append(opts.where, fn)
	}
}

// WithOnClose adds a function to be called before the channel is closed.
func WithOnClose[T any](fn func(ctx context.Context)) ChannelOpt[T] {
	return func(opts *channelOpts[T]) {
		opts.onClose = append(opts.onClose, fn)
	}
}

// NewChannel returns a new Channel with the provided options.
// The channel will be closed when the context is cancelled.
func NewChannel[T any](ctx context.Context, opts ...ChannelOpt[T]) *Channel[T] {
	closed := int64(0)
	var options = &channelOpts[T]{}
	for _, opt := range opts {
		opt(options)
	}
	receiver := &ChannelReceiver[T]{
		ch:        make(chan T, options.bufferSize),
		ctx:       NewMultiContext(ctx),
		closed:    &closed,
		onRcv:     options.onRcv,
		where:     options.where,
		closeOnce: sync.Once{},
		onClose:   options.onClose,
	}
	c := &Channel[T]{
		onSend:          options.onSend,
		ChannelReceiver: receiver,
	}
	go func() {
		ctx, cancel := context.WithCancel(c.ctx)
		defer cancel()
		<-ctx.Done()
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		c.Close(ctx)
	}()
	return c
}

// Context returns the context of the channel.
func (c *Channel[T]) Context() *MultiContext {
	return c.ctx
}

// SendAsync sends a value to the channel in a goroutine. If the channel is closed, it will return false to the channel returned by this function.
// If the context is canceled, it will return false to the channel returned by this function.
// If the value is sent, it will return true to the channel returned by this function.
// This is a non-blocking call.
func (c *Channel[T]) SendAsync(ctx context.Context, value T) chan bool {
	ch := make(chan bool, 1)
	c.wg.Add(1)
	go func(value T) {
		defer c.wg.Done()
		if atomic.LoadInt64(c.closed) > 0 {
			ch <- false
			return
		}
		ctx = c.ctx.WithContext(ctx)
		for _, fn := range c.onSend {
			value = fn(ctx, value)
		}
		select {
		case c.ch <- value:
			ch <- true
		case <-ctx.Done():
			ch <- false
		}
	}(value)
	return ch
}

// Send sends a value to the channel. If the channel is closed or the context is cancelled, it will return false.
// If the value is sent, it will return true.
// This is a blocking call.
func (c *Channel[T]) Send(ctx context.Context, value T) bool {
	if atomic.LoadInt64(c.closed) > 0 {
		return false
	}
	ctx = c.ctx.WithContext(ctx)
	for _, fn := range c.onSend {
		value = fn(ctx, value)
	}
	select {
	case c.ch <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// Recv returns the next value from the channel. If the channel is closed, it will return false.
func (c *ChannelReceiver[T]) Recv(ctx context.Context) (T, bool) {
	if atomic.LoadInt64(c.closed) > 0 {
		return *new(T), false
	}
	ctx = c.ctx.WithContext(ctx)
	select {
	case value, ok := <-c.ch:
		if !ok {
			return *new(T), false
		}
		for _, fn := range c.onRcv {
			value = fn(ctx, value)
		}
		for _, fn := range c.where {
			if !fn(ctx, value) {
				return *new(T), false
			}
		}
		return value, true
	case <-ctx.Done():
		return *new(T), false
	}
}

// ProxyFrom proxies values from the given channel to this channel.
// This is a non-blocking call.
func (c *Channel[T]) ProxyFrom(ctx context.Context, ch *Channel[T]) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			value, ok := ch.Recv(ctx)
			if !ok {
				return
			}
			c.Send(ctx, value)
		}
	}()
}

// ProxyTo proxies values from this channel to the given channel.
// This is a non-blocking call.
func (c *ChannelReceiver[T]) ProxyTo(ctx context.Context, ch *Channel[T]) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			value, ok := c.Recv(ctx)
			if !ok {
				return
			}
			ch.Send(ctx, value)
		}
	}()
}

// Len returns the number of values in the channel.
func (c *ChannelReceiver[T]) Len() int {
	return len(c.ch)
}

// ForEach calls the given function for each value in the channel until the channel is closed, the context is cancelled, or the function returns false.
func (c *ChannelReceiver[T]) ForEach(ctx context.Context, fn func(context.Context, T) bool) {
	for {
		value, ok := c.Recv(ctx)
		if !ok {
			return
		}
		if !fn(ctx, value) {
			return
		}
	}
}

// ForEachAsync calls the given function for each value in the channel until the channel is closed, the context is cancelled, or the function returns false.
// It will call the function in a new goroutine for each value.
func (c *ChannelReceiver[T]) ForEachAsync(ctx context.Context, fn func(context.Context, T) bool) {
	for {
		value, ok := c.Recv(ctx)
		if !ok {
			return
		}
		c.wg.Add(1)
		go func(value T) {
			defer c.wg.Done()
			if !fn(ctx, value) {
				return
			}
		}(value)
	}
}

// Close closes the channel. It will call the OnClose functions and wait for all goroutines to finish.
// If the context is cancelled, waiting for goroutines to finish will be cancelled.
func (c *ChannelReceiver[T]) Close(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.closeOnce.Do(func() {
		done := make(chan struct{}, 1)
		c.ctx.Cancel()
		atomic.StoreInt64(c.closed, 1)
		go func() {
			c.wg.Wait()
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-ctx.Done():
		}
		for _, fn := range c.onClose {
			fn(c.ctx)
		}
		close(c.ch)
	})
}
