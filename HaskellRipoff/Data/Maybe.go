package Data;

import "raft-go/HaskellRipoff/Base"

type Maybe[T comparable] struct {
	hasValue bool
	value T
}

func Nothing[T comparable]() Maybe[T] {
	return Maybe[T] { hasValue: false }
}

func Just[T comparable](value T) Maybe[T] {
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

func (maybe *Maybe[T]) EqualOrNothing(value any) bool {
	if !maybe.hasValue {
		return true
	}

	return maybe.value == value
}


