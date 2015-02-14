package goindex

import (
	"github.com/google/btree"
)

const queryLimitRunningAverageRate = 0.01

type rangeCondition struct {
	indexKey uint32

	tree           *treeIndex
	greaterOrEqual btree.Item
	lessThan       btree.Item

	score float32
}

func newRangeCondition(goIndex *GoIndex, name string, greaterOrEqual, lessThan btree.Item) *rangeCondition {
	indexKey, ok := goIndex.indexKeys[name]
	if !ok {
		return nil
	}

	tree, ok := goIndex.index[indexKey]
	if !ok {
		return nil
	}

	return &rangeCondition{
		indexKey:       indexKey,
		tree:           tree,
		greaterOrEqual: greaterOrEqual,
		lessThan:       lessThan,
	}
}

func (c *rangeCondition) Match(item btree.Item) bool {
	if !item.Less(c.lessThan) || item.Less(c.greaterOrEqual) {
		return false
	}
	return true
}

func (c *rangeCondition) Score() float32 {
	return c.score
}

func (c *rangeCondition) CalcScore(index *GoIndex) {
	if qs, ok := index.queyStats[c.indexKey]; ok {
		c.score = qs
	} else {
		c.score = c.tree.avgQueryLimit
	}
}

func (c *rangeCondition) UpdateScore(index *GoIndex, selectedCount int) {
	limitRate := float32(selectedCount) / float32(c.tree.count)

	c.tree.avgQueryLimit += limitRate * queryLimitRunningAverageRate
	c.tree.avgQueryLimit *= 1 - queryLimitRunningAverageRate

	index.queyStats[c.indexKey] = limitRate
}

func (c *rangeCondition) Key() uint32 {
	return c.indexKey
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
