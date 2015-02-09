package docindex

import (
	"fmt"

	"github.com/google/btree"
)

type Int int

func (a Int) Less(b btree.Item) bool {
	switch b.(type) {
	case Int:
		return a < b.(Int)
	case *leaf:
		return a < b.(*leaf).key.(Int)
	}
	panic(fmt.Sprintf("Invalid type: %q", b))
}

type Float float64

func (a Float) Less(b btree.Item) bool {
	switch b.(type) {
	case Float:
		return a < b.(Float)
	case *leaf:
		return a < b.(*leaf).key.(Float)
	}
	panic(fmt.Sprintf("Invalid type: %q", b))
}

type String string

func (a String) Less(b btree.Item) bool {
	switch b.(type) {
	case String:
		return a < b.(String)
	case *leaf:
		return a < b.(*leaf).key.(String)
	}
	panic(fmt.Sprintf("Invalid type: %q", b))
}
