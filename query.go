package goindex

import (
	"sort"
	"sync"

	"github.com/google/btree"
)

type conditioner interface {
	Match(item btree.Item) bool
	Iter(func(*Doc) bool)

	IndexKey() uint32

	Score() float32
	CalcScore(*GoIndex)
	UpdateScore(*GoIndex, int)

	Destruct()
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

var queryPool = sync.Pool{}

func NewQuery(index *GoIndex) *Query {
	var q *Query

	if v := queryPool.Get(); v != nil {
		q = v.(*Query)
		q.goIndex = index
		q.conditions = q.conditions[:0]
	} else {
		q = &Query{
			goIndex:    index,
			conditions: make([]conditioner, 0, 10),
		}
	}

	return q
}

var itemSlicePool = sync.Pool{}

func newItemSlice() []btree.Item {
	if v := itemSlicePool.Get(); v != nil {
		return v.([]btree.Item)[:0]
	}
	return make([]btree.Item, 0, 10)
}

func (q *Query) ItemRangeFilter(name string, greaterOrEqual, lessThan btree.Item) *Query {
	c := newRangeCondition(q.goIndex, name, greaterOrEqual, lessThan)
	if c != nil {
		q.conditions = append(q.conditions, c)
	}
	return q
}
func (q *Query) SmallIntRangeFilter(name string, greaterOrEqual, lessThan int) *Query {
	return q.ItemRangeFilter(name, SmallInt(greaterOrEqual), SmallInt(lessThan))
}

func (q *Query) IntRangeFilter(name string, greaterOrEqual, lessThan int) *Query {
	return q.ItemRangeFilter(name, Int(greaterOrEqual), Int(lessThan))
}

func (q *Query) FloatRangeFilter(name string, greaterOrEqual, lessThan float64) *Query {
	return q.ItemRangeFilter(name, Float(greaterOrEqual), Float(lessThan))
}

func (q *Query) StringRangeFilter(name string, greaterOrEqual, lessThan string) *Query {
	return q.ItemRangeFilter(name, String(greaterOrEqual), String(lessThan))
}

func (q *Query) ItemInFilter(name string, items ...btree.Item) *Query {
	c := newInCondition(q.goIndex, name, items)
	if c != nil {
		q.conditions = append(q.conditions, c)
	}
	return q
}

func (q *Query) IntInFilter(name string, items ...int) *Query {
	s := newItemSlice()
	for _, item := range items {
		s = append(s, Int(item))
	}

	return q.ItemInFilter(name, s...)
}

func (q *Query) SmallIntInFilter(name string, items ...int) *Query {
	s := newItemSlice()
	for _, item := range items {
		s = append(s, SmallInt(item))
	}

	return q.ItemInFilter(name, s...)
}

func (q *Query) FloatInFilter(name string, items ...float64) *Query {
	s := newItemSlice()
	for _, item := range items {
		s = append(s, Float(item))
	}
	return q.ItemInFilter(name, s...)
}

func (q *Query) StringInFilter(name string, items ...string) *Query {
	s := newItemSlice()
	for _, item := range items {
		s = append(s, String(item))
	}
	return q.ItemInFilter(name, s...)
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
			item, ok := doc.keys[c.IndexKey()]
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

	queryPool.Put(q)
	for _, c := range q.conditions {
		c.Destruct()
	}

	return results
}
