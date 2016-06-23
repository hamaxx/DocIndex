package goindex

import (
	"sync/atomic"

	"github.com/google/btree"
)

type GoIndex struct {
	index map[uint32]*treeIndex

	indexKeys    map[string]uint32
	nextIndexKey uint32

	queryStats map[interface{}]float32 // TODO: lru
}

func New() *GoIndex {
	return &GoIndex{
		index:      make(map[uint32]*treeIndex, 10),
		indexKeys:  make(map[string]uint32, 10),
		queryStats: make(map[interface{}]float32, 1000),
	}
}

func (index *GoIndex) Query() *Query {
	return NewQuery(index)
}

func (index *GoIndex) IndexKeys() map[string]uint32 {
	return index.indexKeys
}

func (index *GoIndex) addItem(indexKey uint32, value btree.Item, doc *Doc) *treeIndex {
	tree, ok := index.index[indexKey]
	if !ok {
		tree = newTreeIndex()
		index.index[indexKey] = tree
	}
	tree.insert(value, doc)
	return tree
}

type Doc struct {
	value   interface{}
	keys    map[uint32]btree.Item
	goIndex *GoIndex
}

func (index *GoIndex) NewDoc(value interface{}) *Doc {
	return &Doc{
		value:   value,
		keys:    map[uint32]btree.Item{},
		goIndex: index,
	}
}

func (doc *Doc) Delete() {
	for itemKey, item := range doc.keys {
		for docK, docF := range doc.goIndex.index[itemKey].docs[item] {
			if docF.value == doc.value {
				doc.goIndex.index[itemKey].docs[item] = append(doc.goIndex.index[itemKey].docs[item][:docK], doc.goIndex.index[itemKey].docs[item][docK+1:]...)
				doc.goIndex.index[itemKey].count--
				if doc.goIndex.index[itemKey].count == 0 {
					// delete from btree index
					doc.goIndex.index[itemKey].tree.Delete(item)
				}
			}
		}
	}
}

func (doc *Doc) Value() interface{} {
	return doc.value
}

func (doc *Doc) ItemKey(name string, item btree.Item) *Doc {
	indexKey, ok := doc.goIndex.indexKeys[name]
	if !ok {
		indexKey = atomic.AddUint32(&doc.goIndex.nextIndexKey, 1)
		doc.goIndex.indexKeys[name] = indexKey
	}

	doc.goIndex.addItem(indexKey, item, doc)
	doc.keys[indexKey] = item
	return doc
}

func (doc *Doc) IntKey(name string, value int) *Doc {
	return doc.ItemKey(name, (Int)(value))
}

func (doc *Doc) SmallIntKey(name string, value int8) *Doc {
	return doc.ItemKey(name, (SmallInt)(value))
}

func (doc *Doc) FloatKey(name string, value float64) *Doc {
	return doc.ItemKey(name, (Float)(value))
}

func (doc *Doc) StringKey(name string, value string) *Doc {
	return doc.ItemKey(name, (String)(value))
}

type treeIndex struct {
	tree *btree.BTree
	docs map[btree.Item][]*Doc

	count         int
	avgQueryLimit float32
}

func newTreeIndex() *treeIndex {
	return &treeIndex{
		tree:          btree.New(2),
		docs:          make(map[btree.Item][]*Doc, 1000),
		count:         0,
		avgQueryLimit: 0,
	}
}

func (t *treeIndex) insert(value btree.Item, doc *Doc) {
	r := t.tree.Get(value)
	if r == nil {
		r = value
		t.tree.ReplaceOrInsert(value)
	}
	t.docs[r] = append(t.docs[r], doc)
	t.count++
}
