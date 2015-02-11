package goindex

import "github.com/google/btree"

type condition struct {
	tree                     *btree.BTree
	greaterOrEqual, lessThan btree.Item
}

type Query struct {
	conditions []*condition
	goIndex    *GoIndex

	tmpLeafL *leaf
	tmpLeafG *leaf
}

func NewQuery(index *GoIndex) *Query {
	return &Query{
		goIndex:    index,
		conditions: []*condition{},
	}
}

func (q *Query) ItemFilter(name string, greaterOrEqual, lessThan btree.Item) *Query {
	tree, ok := q.goIndex.index[name]
	if !ok {
		return q
	}
	q.conditions = append(q.conditions, &condition{tree, greaterOrEqual, lessThan})
	return q
}

func (q *Query) IntFilter(name string, greaterOrEqual, lessThan int) *Query {
	return q.ItemFilter(name, (Int)(greaterOrEqual), (Int)(lessThan))
}

func (q *Query) FloatFilter(name string, greaterOrEqual, lessThan float64) *Query {
	return q.ItemFilter(name, (Float)(greaterOrEqual), (Float)(lessThan))
}

func (q *Query) StringFilter(name string, greaterOrEqual, lessThan string) *Query {
	return q.ItemFilter(name, (String)(greaterOrEqual), (String)(lessThan))
}

func (q *Query) Exec() []*Doc {
	// TODO: Proper query plan and exec, this is a proof of concept

	var limiter *condition
	for _, c := range q.conditions {
		if limiter == nil || c.tree.Len() > limiter.tree.Len() {
			limiter = c
		}
	}

	results := []*Doc{}

	if limiter == nil {
		return results
	}

	limiter.tree.AscendRange(newQueryLeaf(limiter.greaterOrEqual), newQueryLeaf(limiter.lessThan), func(item btree.Item) bool {
		leaf := item.(*leaf)
		docs := leaf.docs
		for _, doc := range docs {
			match := true
			for _, c := range q.conditions {
				if c == limiter {
					continue
				}
				item, ok := doc.keys[c.tree]
				if !ok {
					match = false
					break
				}
				if !item.Less(c.lessThan) || item.Less(c.greaterOrEqual) {
					match = false
					break
				}
			}
			if match {
				results = append(results, doc)
			}
		}
		return true
	})

	return results
}
