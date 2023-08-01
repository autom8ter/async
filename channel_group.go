package async

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ChannelBroadcast is a thread-safe group of channels. It is useful for broadcasting a value to multiple channels at once.
type ChannelBroadcast[T any] struct {
	ctx         *MultiContext
	subscribers sync.Map
	count       *int64
}

// NewChannelBroadcast returns a new ChannelBroadcast. The context is used to cancel all subscribers when the context is canceled.
// A channel group is useful for broadcasting a value to multiple subscribers.
func NewChannelBroadcast[T any](ctx context.Context) *ChannelBroadcast[T] {
	count := int64(0)
	return &ChannelBroadcast[T]{
		ctx:         NewMultiContext(ctx),
		subscribers: sync.Map{},
		count:       &count,
	}
}

// SendAsync sends a value to all channels in the group asynchronously. This is a non-blocking operation.
func (cg *ChannelBroadcast[T]) SendAsync(ctx context.Context, val T) chan bool {
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
func (cg *ChannelBroadcast[T]) Send(ctx context.Context, val T) {
	cg.subscribers.Range(func(key, state any) bool {
		c := state.(*Channel[T])
		c.Send(ctx, val)
		return true
	})
	return
}

// Channel returns a channel that will receive values from broadcasted values. The channel will be closed when the context is canceled.
// This is a non-blocking operation.
func (b *ChannelBroadcast[T]) Channel(ctx context.Context, opts ...ChannelOpt[T]) *ChannelReceiver[T] {
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
func (c *ChannelBroadcast[T]) Len() int {
	var count = 0
	c.subscribers.Range(func(key, state any) bool {
		count++
		return true
	})
	return count
}

// Close blocks until all subscribers have been removed and then closes the broadcast.
func (b *ChannelBroadcast[T]) Close() {
	b.ctx.Cancel()
	for b.Len() > 0 {
		time.Sleep(time.Millisecond * 10)
	}
}
