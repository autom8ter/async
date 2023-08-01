package async

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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
