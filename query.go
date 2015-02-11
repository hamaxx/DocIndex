package goindex

import (
	"sort"

	"github.com/google/btree"
)

const queryLimitRunningAverageRate = 0.01

type condition struct {
	tree                     *treeIndex
	greaterOrEqual, lessThan btree.Item

	score float32
}

type conditionScore []*condition

func (v conditionScore) Len() int {
	return len(v)
}
func (v conditionScore) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
func (v conditionScore) Less(i, j int) bool {
	return v[i].score < v[j].score
}

type Query struct {
	conditions []*condition
	goIndex    *GoIndex
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
	q.conditions = append(q.conditions, &condition{
		tree:           tree,
		greaterOrEqual: greaterOrEqual,
		lessThan:       lessThan,
	})
	return q
}

func (q *Query) IntRangeFilter(name string, greaterOrEqual, lessThan int) *Query {
	return q.ItemFilter(name, (Int)(greaterOrEqual), (Int)(lessThan))
}

func (q *Query) FloatRangeFilter(name string, greaterOrEqual, lessThan float64) *Query {
	return q.ItemFilter(name, (Float)(greaterOrEqual), (Float)(lessThan))
}

func (q *Query) StringRangeFilter(name string, greaterOrEqual, lessThan string) *Query {
	return q.ItemFilter(name, (String)(greaterOrEqual), (String)(lessThan))
}

func (q *Query) findBestLimiter() (*condition, []*condition) {
	if len(q.conditions) == 0 {
		return nil, nil
	}
	if len(q.conditions) == 1 {
		return q.conditions[0], nil
	}

	for _, c := range q.conditions {
		if qs, ok := q.goIndex.queyStats[*c]; ok {
			c.score = qs
			continue
		}
		c.score = c.tree.avgQueryLimit
	}

	sort.Sort(conditionScore(q.conditions))

	return q.conditions[0], q.conditions[1:]
}

func (q *Query) updateQueryStats(limiter *condition, selectedCount int) {
	limitRate := float32(selectedCount) / float32(limiter.tree.count)

	limiter.tree.avgQueryLimit += limitRate * queryLimitRunningAverageRate
	limiter.tree.avgQueryLimit *= 1 - queryLimitRunningAverageRate

	q.goIndex.queyStats[*limiter] = limitRate
}

func (q *Query) Exec() []*Doc {
	results := []*Doc{}

	limiter, filters := q.findBestLimiter()
	if limiter == nil {
		return results
	}

	rangeSize := 0
	limiter.tree.tree.AscendRange(limiter.greaterOrEqual, limiter.lessThan, func(res btree.Item) bool {
		docs := limiter.tree.docs[res]
	docLoop:
		for _, doc := range docs {
			rangeSize++
			for _, c := range filters {
				item, ok := doc.keys[c.tree]
				if !ok {
					continue docLoop
				}

				if !item.Less(c.lessThan) || item.Less(c.greaterOrEqual) {
					continue docLoop
				}
			}
			results = append(results, doc)
		}
		return true
	})

	q.updateQueryStats(limiter, rangeSize)

	return results
}
