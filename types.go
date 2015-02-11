package goindex

import (
	"github.com/google/btree"
)

type Int int

func (a Int) Less(b btree.Item) bool {
	return a < b.(Int)
}

type Float float64

func (a Float) Less(b btree.Item) bool {
	return a < b.(Float)
}

type String string

func (a String) Less(b btree.Item) bool {
	return a < b.(String)
}
