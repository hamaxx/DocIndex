package goindex

import (
	"github.com/google/btree"
)

type inCondition struct {
	tree     *treeIndex
	indexKey uint32
	in       map[btree.Item]struct{}

	score float32
}

func newInCondition(goIndex *GoIndex, name string, items []btree.Item) *inCondition {
	indexKey, ok := goIndex.indexKeys[name]
	if !ok {
		return nil
	}

	tree, ok := goIndex.index[indexKey]
	if !ok {
		return nil
	}

	inMap := make(map[btree.Item]struct{}, len(items))
	for _, item := range items {
		inMap[item] = struct{}{}
	}

	return &inCondition{
		tree:     tree,
		indexKey: indexKey,
		in:       inMap,
	}
}

func (c *inCondition) Match(item btree.Item) bool {
	_, ok := c.in[item]
	return ok
}

func (c *inCondition) Iter(cb func(*Doc) bool) {
	for item := range c.in {
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
	for item := range c.in {
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
