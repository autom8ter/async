package async_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autom8ter/async"
	"github.com/stretchr/testify/assert"
)

func init() {
	os.Setenv("ASYNC_DEBUG", "true")
}

func TestNewChannelGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g := async.NewChannelGroup[string](ctx)
	wg := sync.WaitGroup{}
	count := int64(0)
	mu := sync.Mutex{}
	var received = map[string]bool{}
	var duplicateErr error
	for i := 0; i < 100; i++ {
		wg.Add(1)
		ch := g.Channel(ctx)
		go func(i int, ch *async.ChannelReceiver[string]) {
			defer wg.Done()
			for duplicateErr == nil {
				value, ok := ch.Recv(ctx)
				if !ok {
					return
				}
				mu.Lock()
				if _, ok := received[fmt.Sprintf("%v-%v", i, value)]; ok {
					duplicateErr = fmt.Errorf("duplicate value %v", value)
					mu.Unlock()
					return
				}
				assert.NotNil(t, value)
				atomic.AddInt64(&count, 1)
				received[fmt.Sprintf("%v-%v", i, value)] = true
				mu.Unlock()
			}
		}(i, ch)
	}

	assert.Equal(t, g.Len(), 100)
	for i := 0; i < 100; i++ {
		<-g.SendAsync(ctx, fmt.Sprintf("node-%d", i))
	}
	time.Sleep(1 * time.Second)
	g.Close()
	assert.NoError(t, duplicateErr)
	wg.Wait()
	assert.Equal(t, 0, g.Len())
	assert.Equal(t, int64(10000), count)
}

func TestBorrower(t *testing.T) {
	b := async.NewBorrower[string]("testing")
	value := b.Borrow()
	assert.EqualValues(t, "testing", *value)
	assert.NoError(t, b.Return(value))
	assert.Error(t, b.Return(value))
	value = b.Borrow()
	assert.EqualValues(t, "testing", *value)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := b.BorrowContext(ctx)
	assert.Error(t, err)
	assert.NoError(t, b.Return(value))
	assert.NoError(t, b.Do(func(value *string) {
		*value = "testing2"
	}))
	value = b.Borrow()
	assert.EqualValues(t, "testing2", *value)
	assert.EqualValues(t, "testing2", b.Value())
	assert.NoError(t, b.Close())
}
