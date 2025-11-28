package Base

type Functor[T any] interface {
	Map(f func(T) any) Functor[any];
	Replace(a any) Functor[any];
}

type Semigroup[T any] interface {
	Concat(Semigroup[T]) Semigroup[any];
}

type Monoid[T any] interface {
	// Should include this as static
	Identity() Semigroup[T];
}

//
//type Monad[T any] interface {
//	AndThen() Monoid[any];
//	BindAndThen(func (any) Monad[any]) Monoid[any];
//
//	// Should include this as static
//	Return(T) Monad[any];
//}
//
//type Foldable[T any] interface {
//	// Not real foldable, due to lack of proper higher-kinded data in Go
//	// Also, there's an array; this ain't foldable chief
//
//	// "this" is the initial element used for folding
//	// foldr :: (a -> b -> b) -> b -> t a -> b
//	FoldRight(func (T, T) T, []T) Foldable[T];
//
//	FoldLeft(func (T, T) T, []T) T;
//}

