package storage

import (
	"context"
	"errors"
)

var (
	ErrIteratorDone = errors.New("iterator is done")
)

type IteratorFunc func(ctx context.Context) ([]byte, error)
type YieldFunc func([]byte, error) bool
type MapperFunc func(YieldFunc)
type CancelFunc func()

func Iterator(mapper MapperFunc) (IteratorFunc, CancelFunc) {
	values := make(chan []byte, 1)
	done := make(chan struct{}, 1)
	errs := make(chan error, 1)

	go func() {
		defer close(values)

		mapper(func(value []byte, err error) bool {
			if err != nil {
				errs <- err
				return false
			}

			select {
			case <-done:
				return false
			case values <- value:
				return true
			}
		})
	}()

	iter := func(ctx context.Context) ([]byte, error) {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case err := <-errs:
			return nil, err
		case value, ok := <-values:
			if !ok {
				return nil, ErrIteratorDone
			}
			return value, nil
		}
	}

	return iter, func() {
		close(done)
	}
}
