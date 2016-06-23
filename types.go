package goindex

import (
	"github.com/google/btree"
)

type Int int32

func (a Int) Less(b btree.Item) bool {
	return a < b.(Int)
}

type Float float32

func (a Float) Less(b btree.Item) bool {
	return a < b.(Float)
}

type String string

func (a String) Less(b btree.Item) bool {
	return a < b.(String)
}

type SmallInt int8

func (a SmallInt) Less(b btree.Item) bool {
	return a < b.(SmallInt)
}
