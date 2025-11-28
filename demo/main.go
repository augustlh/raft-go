package main

import "raft-go/HaskellRipoff/Data"
import "raft-go/HaskellRipoff/Base"

func DoubleIt(x int) int {
	return x * 2;
}

func main() {
	var idk Base.Functor[int] = Data.Just(0);
}

