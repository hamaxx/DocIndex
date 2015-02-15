package goindex

import (
	"fmt"
	"math/rand"
	"testing"
)

func init() {
	seed := int64(123)
	rand.Seed(seed)
}

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

func TestDocEmptyIndex(t *testing.T) {
	index := New()
	index.Query().Exec()
	index.Query().IntRangeFilter("len", 1, 3).Exec()

	index.NewDoc("A")
	index.Query().IntRangeFilter("len", 1, 3).Exec()
}
func TestDocRangeIndex(t *testing.T) {
	index := New()

	doc1 := index.NewDoc("A")
	doc1.IntKey("len", 1).StringKey("k", "A")

	doc2 := index.NewDoc("B")
	doc2.IntKey("len", 1).StringKey("k", "B")

	doc3 := index.NewDoc("AA")
	doc3.IntKey("len", 2).StringKey("k", "AA")

	doc4 := index.NewDoc("Num")
	doc4.FloatKey("val", 1.5)

	resFull := index.Query().IntRangeFilter("len", 1, 3).Exec()
	if len(resFull) != 3 {
		t.Fatalf("Invalid result count: %d != 3", len(resFull))
	}
	if !testItems(resFull, []*Doc{doc1, doc2, doc3}) {
		t.Fatalf("Missmatching results")
	}

	res2 := index.Query().IntRangeFilter("len", 1, 2).Exec()
	if len(res2) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(res2))
	}
	if !testItems(res2, []*Doc{doc1, doc2}) {
		t.Fatalf("Missmatching results")
	}

	resA := index.Query().IntRangeFilter("len", 1, 3).StringRangeFilter("k", "A", "B").Exec()
	if len(resA) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(resA))
	}
	if !testItems(resA, []*Doc{doc1, doc3}) {
		t.Fatalf("Missmatching results")
	}

	resFloat := index.Query().FloatRangeFilter("val", 1, 2).Exec()
	if len(resFloat) != 1 {
		t.Fatalf("Invalid result count: %d != 1", len(resFloat))
	}
	if !testItems(resFloat, []*Doc{doc4}) {
		t.Fatalf("Missmatching results")
	}
}

func TestDocInIndex(t *testing.T) {
	index := New()

	doc1 := index.NewDoc("A")
	doc1.IntKey("len", 1).StringKey("k", "A")

	doc2 := index.NewDoc("B")
	doc2.IntKey("len", 1).StringKey("k", "B")

	doc3 := index.NewDoc("AA")
	doc3.IntKey("len", 2).StringKey("k", "AA")

	doc4 := index.NewDoc("Num")
	doc4.FloatKey("val", 1.5)

	resFull := index.Query().IntInFilter("len", 1, 2).Exec()
	if len(resFull) != 3 {
		t.Fatalf("Invalid result count: %d != 3", len(resFull))
	}
	if !testItems(resFull, []*Doc{doc1, doc2, doc3}) {
		t.Fatalf("Missmatching results")
	}

	res2 := index.Query().IntInFilter("len", 1).Exec()
	if len(res2) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(res2))
	}
	if !testItems(res2, []*Doc{doc1, doc2}) {
		t.Fatalf("Missmatching results")
	}

	resA := index.Query().IntInFilter("len", 1, 2, 3).StringInFilter("k", "A", "AA").Exec()
	if len(resA) != 2 {
		t.Fatalf("Invalid result count: %d != 2", len(resA))
	}
	if !testItems(resA, []*Doc{doc1, doc3}) {
		t.Fatalf("Missmatching results")
	}

	resFloat := index.Query().FloatInFilter("val", 1.5).Exec()
	if len(resFloat) != 1 {
		t.Fatalf("Invalid result count: %d != 1", len(resFloat))
	}
	if !testItems(resFloat, []*Doc{doc4}) {
		t.Fatalf("Missmatching results")
	}
}

func BenchmarkInsert(b *testing.B) {
	index := New()
	for i := 0; i < b.N; i++ {
		index.NewDoc("A").IntKey("len", 1).StringKey("k", "A")
	}
}

func BenchmarkRangeFilter(b *testing.B) {
	b.StopTimer()

	index := New()
	for i := 0; i < 10000; i++ {
		doc := index.NewDoc("A")
		for j := 0; j < 100; j++ {
			doc.IntKey(fmt.Sprintf("i%d", j), int(rand.Uint32()%100))
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			index.Query().IntRangeFilter("i1", 0, 50).IntRangeFilter("i2", 0, 20).IntRangeFilter("i3", 0, 1).Exec()
		} else {
			index.Query().IntRangeFilter("i1", 0, 50).IntRangeFilter("i2", 0, 20).IntRangeFilter("i3", 0, 100).Exec()
		}
	}
}

func BenchmarkInFilter(b *testing.B) {
	b.StopTimer()

	index := New()
	for i := 0; i < 10000; i++ {
		doc := index.NewDoc("A")
		for j := 0; j < 100; j++ {
			doc.IntKey(fmt.Sprintf("i%d", j), int(rand.Uint32()%10))
		}
		doc.IntKey("i100", int(rand.Uint32()%100))
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			index.Query().IntInFilter("i1", 0, 1, 2, 3, 4).IntInFilter("i2", 0, 1).IntInFilter("i100", 0).Exec()
		} else {
			index.Query().IntInFilter("i1", 0, 1, 2, 3, 4).IntInFilter("i2", 0, 1).IntInFilter("i3", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9).Exec()
		}
	}
}
