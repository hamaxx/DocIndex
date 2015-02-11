package goindex

import "github.com/google/btree"

type GoIndex struct {
	index map[string]*treeIndex

	queyStats map[condition]float32 // TODO: lru
}

func New() *GoIndex {
	return &GoIndex{
		index:     map[string]*treeIndex{},
		queyStats: map[condition]float32{},
	}
}

func (index *GoIndex) Query() *Query {
	return NewQuery(index)
}

func (index *GoIndex) addItem(name string, value btree.Item, doc *Doc) *treeIndex {
	tree, ok := index.index[name]
	if !ok {
		tree = newTreeIndex()
		index.index[name] = tree
	}
	tree.insert(value, doc)
	return tree
}

func (index *GoIndex) NewDoc(value interface{}) *Doc {
	return &Doc{
		value:   value,
		keys:    map[*treeIndex]btree.Item{},
		goIndex: index,
	}
}

type Doc struct {
	value   interface{}
	keys    map[*treeIndex]btree.Item
	goIndex *GoIndex
}

func (doc *Doc) Value() interface{} {
	return doc.value
}

func (doc *Doc) ItemKey(name string, item btree.Item) *Doc {
	tree := doc.goIndex.addItem(name, item, doc)
	doc.keys[tree] = item
	return doc
}

func (doc *Doc) IntKey(name string, value int) *Doc {
	return doc.ItemKey(name, (Int)(value))
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
		docs:          map[btree.Item][]*Doc{},
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
