/*
Package async is a package for asynchronous programming in Go.

It provides a set of primitives for safely building concurrent applications that don't leak resources or deadlock.
*/
package async

import (
	"context"
	"fmt"
	"sync"
)

// Input is a struct that contains a context and a value.
type Input[T any] struct {
	Ctx   context.Context
	ID    string
	Value T
}

// Output is a struct that contains a context, a value, and an error.
type Output[T any] struct {
	Ctx   context.Context
	Value T
	Err   error
}

// IOFunc is a function that takes a context and an input and returns a result.
type IOFunc[I any, O any] func(input Input[I]) *Output[O]

// InputWrapper is a function that takes an input and returns an input.
type InputWrapper[I any] func(I) I

// WrapInput wraps the input of the IO function with the given input wrappers before calling it.
func (fn IOFunc[I, O]) WrapInput(wrappers ...InputWrapper[I]) IOFunc[I, O] {
	return func(input Input[I]) *Output[O] {
		for _, wrapper := range wrappers {
			input.Value = wrapper(input.Value)
		}
		return fn(input)
	}
}

// OutputWrapper is a function that takes an output and returns an output.
type OutputWrapper[O any] func(*Output[O]) *Output[O]

// WrapOutput wraps the output of the IO function with the given output wrappers before returning it.
func (fn IOFunc[I, O]) WrapOutput(wrappers ...OutputWrapper[O]) IOFunc[I, O] {
	return func(input Input[I]) *Output[O] {
		output := fn(input)
		for _, wrapper := range wrappers {
			output = wrapper(output)
		}
		return output
	}
}

// IOMiddleware is a struct that contains an input wrapper and an output wrapper. It can be used to wrap the input and output of an IO function.
type IOMiddleware[I any, O any] struct {
	Input  func(Input[I]) Input[I]
	Output func(*Output[O]) *Output[O]
}

// IOHandler runs functions asynchronously.
type IOHandler[I any, O any] struct {
	wg      sync.WaitGroup
	ctx     *MultiContext
	handler IOFunc[I, O]
}

// NewIOHandler creates a new IOHandler instance.
func NewIOHandler[I any, O any](ctx context.Context, fn IOFunc[I, O]) *IOHandler[I, O] {
	return &IOHandler[I, O]{
		ctx:     NewMultiContext(ctx),
		wg:      sync.WaitGroup{},
		handler: fn,
	}
}

// WithInputWrappers adds the given input wrappers to the IOHandler instance. This method is not thread-safe.
func (a *IOHandler[I, O]) WithInputWrappers(wrappers ...InputWrapper[I]) *IOHandler[I, O] {
	a.handler = a.handler.WrapInput(wrappers...)
	return a
}

// WithOutputWrappers adds the given output wrappers to the IOHandler instance. This method is not thread-safe.
func (a *IOHandler[I, O]) WithOutputWrappers(wrappers ...OutputWrapper[O]) *IOHandler[I, O] {
	a.handler = a.handler.WrapOutput(wrappers...)
	return a
}

// Process calls the given function in a new goroutine and returns a channel that will receive the result of the function.
func (a *IOHandler[I, O]) Process(input Input[I], opts ...ChannelOpt[*Output[O]]) *Channel[*Output[O]] {
	if input.Ctx == nil {
		input.Ctx = context.Background()
	}
	input.Ctx = a.ctx.WithContext(input.Ctx)
	ch := NewChannel[*Output[O]](input.Ctx, opts...)
	a.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch.Send(input.Ctx, &Output[O]{
					Ctx:   input.Ctx,
					Value: *new(O),
					Err:   fmt.Errorf("panic: %v", r),
				})
			}
			a.wg.Done()
		}()
		ch.Send(input.Ctx, a.handler(Input[I]{
			Ctx:   a.ctx.WithContext(input.Ctx),
			Value: input.Value,
		}))
		ch.Close(context.Background())
	}()
	return ch
}

// ProcessSync calls the given function and returns the result.
func (a *IOHandler[I, O]) ProcessSync(input Input[I]) *Output[O] {
	if input.Ctx == nil {
		input.Ctx = context.Background()
	}
	return a.handler(Input[I]{
		Ctx:   a.ctx.WithContext(input.Ctx),
		Value: input.Value,
	})
}

// ProcessStream calls the given function for each value in the channel until the channel is closed, or the context is cancelled.
func (a *IOHandler[I, O]) ProcessStream(streamCtx context.Context, inputs chan Input[I], opts ...ChannelOpt[*Output[O]]) *Channel[*Output[O]] {
	streamCtx = a.ctx.WithContext(streamCtx)
	ch := NewChannel[*Output[O]](streamCtx, opts...)
	var handleInput = func(input Input[I]) {
		defer func() {
			if r := recover(); r != nil {
				ch.Send(input.Ctx, &Output[O]{
					Ctx:   input.Ctx,
					Value: *new(O),
					Err:   fmt.Errorf("panic: %v", r),
				})
			}
		}()
		ch.Send(streamCtx, a.handler(Input[I]{
			Ctx:   a.ctx.WithContext(input.Ctx),
			Value: input.Value,
		}))
	}
	a.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch.Send(streamCtx, &Output[O]{
					Ctx:   streamCtx,
					Value: *new(O),
					Err:   fmt.Errorf("panic: %v", r),
				})
			}
			ch.Close(context.Background())
			a.wg.Done()
		}()
		ctx, cancel := context.WithCancel(streamCtx)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case input := <-inputs:
				if input.Ctx == nil {
					input.Ctx = streamCtx
				}
				handleInput(input)
			}
		}
	}()
	return ch
}

// Close cancels the root context and waits for all goroutines to finish.
func (a *IOHandler[I, O]) Close() {
	a.ctx.Cancel()
	a.wg.Wait()
}

// Wait waits for all goroutines to finish.
func (a *IOHandler[I, O]) Wait() {
	a.wg.Wait()
}
