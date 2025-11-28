package Data;

import "raft-go/HaskellRipoff/Base"

type Maybe[T any] struct {
	hasValue bool
	value T
}

func Nothing[T any]() Maybe[T] {
	return Maybe[T] { hasValue: false }
}

func Just[T any](value T) Maybe[T] {
	return Maybe[T] { hasValue: true, value: value }
}
func (maybe *Maybe[T]) FromJust() T {
	if !maybe.hasValue {
		panic("Maybe is nothing")
	}
	return maybe.value
}

func (maybe *Maybe[T]) IsJust() bool {
	return maybe.hasValue
}

func (maybe *Maybe[T]) IsNothing() bool {
	return !maybe.hasValue
}

func (maybe Maybe[T]) Map(f func (T) any) Base.Functor[any] {
	if !maybe.hasValue {
		return Nothing[any]()
	}

	return Just(f(maybe.value))
}

func (maybe Maybe[T]) Replace(a any) Base.Functor[any] {
	if !maybe.hasValue {
		return Nothing[any]()
	}

	return Just(a)
}


