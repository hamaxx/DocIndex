package goindex

import (
	"sync"

	"github.com/google/btree"
)

type inCondition struct {
	tree     *treeIndex
	indexKey uint32
	in       []btree.Item

	score float32
}

var inConditionPool = sync.Pool{}

func newInCondition(goIndex *GoIndex, name string, items []btree.Item) *inCondition {
	var c *inCondition

	if v := inConditionPool.Get(); c != nil {
		c = v.(*inCondition)
	} else {
		c = &inCondition{}
	}

	indexKey, ok := goIndex.indexKeys[name]
	if !ok {
		return nil
	}

	tree, ok := goIndex.index[indexKey]
	if !ok {
		return nil
	}

	c.in = items
	c.indexKey = indexKey
	c.tree = tree

	return c
}

func (c *inCondition) Match(item btree.Item) bool {
	// For small list linear search is faster than maps
	for _, valid := range c.in {
		if item == valid {
			return true
		}
	}
	return false
}

func (c *inCondition) Iter(cb func(*Doc) bool) {
	for _, item := range c.in {
		docs, ok := c.tree.docs[item]
		if ok {
			for _, doc := range docs {
				if !cb(doc) {
					return
				}
			}
		}
	}
}

func (c *inCondition) Score() float32 {
	return c.score
}

func (c *inCondition) CalcScore(index *GoIndex) {
	sc := 0
	for _, item := range c.in {
		docs, ok := c.tree.docs[item]
		if ok {
			sc += len(docs)
		}
	}
	c.score = float32(sc) / float32(c.tree.count)
}

func (c *inCondition) UpdateScore(index *GoIndex, selectedCount int) {
}

func (c *inCondition) IndexKey() uint32 {
	return c.indexKey
}

func (c *inCondition) Destruct() {
	itemSlicePool.Put(c.in)
	inConditionPool.Put(c)
}
