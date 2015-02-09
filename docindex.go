package docindex

import (
	"github.com/google/btree"
)

type DocIndex struct {
	index map[string]*btree.BTree
}

type Doc struct {
	value    interface{}
	keys     map[*btree.BTree]btree.Item
	docIndex *DocIndex
}

type leaf struct {
	key  btree.Item
	docs []*Doc
}

func (l *leaf) Less(than btree.Item) bool {
	return l.key.Less(than.(*leaf).key)
}

func newLeaf(key btree.Item, doc *Doc) *leaf {
	return &leaf{key: key, docs: []*Doc{doc}}
}

func newQueryLeaf(key btree.Item) *leaf {
	return &leaf{key: key}
}

func New() *DocIndex {
	return &DocIndex{
		index: map[string]*btree.BTree{},
	}
}

func (index *DocIndex) Query() *Query {
	return NewQuery(index)
}

func (index *DocIndex) addItem(name string, value btree.Item, doc *Doc) *btree.BTree {
	tree, ok := index.index[name]
	if !ok {
		tree = btree.New(2)
		index.index[name] = tree
	}
	r := tree.Get(newQueryLeaf(value))
	if r == nil {
		tree.ReplaceOrInsert(newLeaf(value, doc))
	} else {
		leaf := r.(*leaf)
		leaf.docs = append(leaf.docs, doc)
	}
	return tree
}

func (index *DocIndex) NewDoc(value interface{}) *Doc {
	return &Doc{
		value:    value,
		keys:     map[*btree.BTree]btree.Item{},
		docIndex: index,
	}
}

func (doc *Doc) Value() interface{} {
	return doc.value
}

func (doc *Doc) ItemKey(name string, item btree.Item) *Doc {
	tree := doc.docIndex.addItem(name, item, doc)
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
