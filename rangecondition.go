package goindex

import (
	"sync"

	"github.com/google/btree"
)

const queryLimitRunningAverageRate = 0.01

type rangeCondition struct {
	tree           *treeIndex
	indexKey       uint32
	greaterOrEqual btree.Item
	lessThan       btree.Item

	score float32
}

var rangeConditionPool = sync.Pool{}

func newRangeCondition(goIndex *GoIndex, name string, greaterOrEqual, lessThan btree.Item) *rangeCondition {
	var c *rangeCondition

	if v := rangeConditionPool.Get(); c != nil {
		c = v.(*rangeCondition)
	} else {
		c = &rangeCondition{}
	}

	indexKey, ok := goIndex.indexKeys[name]
	if !ok {
		return nil
	}

	tree, ok := goIndex.index[indexKey]
	if !ok {
		return nil
	}

	c.tree = tree
	c.indexKey = indexKey
	c.greaterOrEqual = greaterOrEqual
	c.lessThan = lessThan

	return c
}

func (c *rangeCondition) Match(item btree.Item) bool {
	if !item.Less(c.lessThan) || item.Less(c.greaterOrEqual) {
		return false
	}
	return true
}

func (c *rangeCondition) Iter(cb func(*Doc) bool) {
	c.tree.tree.AscendRange(c.greaterOrEqual, c.lessThan, func(res btree.Item) bool {
		for _, doc := range c.tree.docs[res] {
			if !cb(doc) {
				return false
			}
		}
		return true
	})
}

func (c *rangeCondition) Score() float32 {
	return c.score
}

func (c *rangeCondition) CalcScore(index *GoIndex) {
	if qs, ok := index.queryStats[c.ConditionKey()]; ok {
		c.score = qs
	} else {
		c.score = c.tree.avgQueryLimit
	}
}

func (c *rangeCondition) UpdateScore(index *GoIndex, selectedCount int) {
	limitRate := float32(selectedCount) / float32(c.tree.count)

	c.tree.avgQueryLimit += limitRate * queryLimitRunningAverageRate
	c.tree.avgQueryLimit *= 1 - queryLimitRunningAverageRate

	index.queryStats[c.ConditionKey()] = limitRate
}

func (c *rangeCondition) ConditionKey() interface{} {
	return *c
}

func (c *rangeCondition) IndexKey() uint32 {
	return c.indexKey
}

func (c *rangeCondition) Destruct() {
	rangeConditionPool.Put(c)
}
