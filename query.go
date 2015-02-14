package goindex

import (
	"sort"

	"github.com/google/btree"
)

type conditioner interface {
	Match(item btree.Item) bool
	Iter(func(*Doc) bool)

	Key() uint32

	Score() float32
	CalcScore(*GoIndex)
	UpdateScore(*GoIndex, int)
}

type conditionScore []conditioner

func (v conditionScore) Len() int {
	return len(v)
}
func (v conditionScore) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}
func (v conditionScore) Less(i, j int) bool {
	return v[i].Score() < v[j].Score()
}

type Query struct {
	conditions []conditioner
	goIndex    *GoIndex
}

func NewQuery(index *GoIndex) *Query {
	return &Query{
		goIndex:    index,
		conditions: make([]conditioner, 0, 10),
	}
}

func (q *Query) ItemFilter(name string, greaterOrEqual, lessThan btree.Item) *Query {
	c := newRangeCondition(q.goIndex, name, greaterOrEqual, lessThan)
	if c != nil {
		q.conditions = append(q.conditions, c)
	}
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

func (q *Query) findBestLimiter() (conditioner, []conditioner) {
	if len(q.conditions) == 0 {
		return nil, nil
	}
	if len(q.conditions) == 1 {
		return q.conditions[0], nil
	}

	for _, c := range q.conditions {
		c.CalcScore(q.goIndex)
	}

	sort.Sort(conditionScore(q.conditions))

	return q.conditions[0], q.conditions[1:]
}

func (q *Query) Exec() []*Doc {
	results := []*Doc{}

	limiter, filters := q.findBestLimiter()
	if limiter == nil {
		return results
	}

	rangeSize := 0
	limiter.Iter(func(doc *Doc) bool {
		rangeSize++
		for _, c := range filters {
			item, ok := doc.keys[c.Key()]
			if !ok {
				return true
			}

			if !c.Match(item) {
				return true
			}
		}
		results = append(results, doc)
		return true
	})

	limiter.UpdateScore(q.goIndex, rangeSize)

	return results
}
