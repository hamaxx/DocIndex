package docindex

import (
	"testing"
)

func testItems(a, b []*Doc) bool {
	m := map[*Doc]int{}
	for _, d := range b {
		m[d]++
	}
	for _, d := range a {
		m[d]--
	}
	for _, c := range m {
		if c != 0 {
			return false
		}
	}
	return true
}

func TestDocIndex(t *testing.T) {
	index := New()

	doc1 := index.NewDoc("A")
	doc1.IntKey("len", 1).StringKey("k", "A")

	doc2 := index.NewDoc("B")
	doc2.IntKey("len", 1).StringKey("k", "B")

	doc3 := index.NewDoc("AA")
	doc3.IntKey("len", 2).StringKey("k", "AA")

	doc4 := index.NewDoc("Num")
	doc4.FloatKey("val", 1.5)

	resFull := index.Query().IntFilter("len", 1, 3).Exec()
	if len(resFull) != 3 {
		t.Fatalf("Invalid result count: %d != 3", len(resFull))
	}
	if !testItems(resFull, []*Doc{doc1, doc2, doc3}) {
		t.Fatalf("Missmatching results")
	}

	res2 := index.Query().IntFilter("len", 1, 2).Exec()
	if len(res2) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(res2))
	}
	if !testItems(res2, []*Doc{doc1, doc2}) {
		t.Fatalf("Missmatching results")
	}

	resA := index.Query().IntFilter("len", 1, 3).StringFilter("k", "A", "B").Exec()
	if len(resA) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(resA))
	}
	if !testItems(resA, []*Doc{doc1, doc3}) {
		t.Fatalf("Missmatching results")
	}

	resFloat := index.Query().FloatFilter("val", 1, 2).Exec()
	if len(resFloat) != 1 {
		t.Fatalf("Invalid result count: %d != 1", len(resFloat))
	}
	if !testItems(resFloat, []*Doc{doc4}) {
		t.Fatalf("Missmatching results")
	}
}
