# GoIndex
In-memory document search

**This is an experimental project so use it as such**

### Example ###

    import "github.com/hamaxx/goindex"

    index := goindex.New()

	index.NewDoc("A").IntKey("len", 1).StringKey("k", "A")
	index.NewDoc("B").IntKey("len", 1).StringKey("k", "B")
	index.NewDoc("AA").IntKey("len", 2).StringKey("k", "AA")

	resFull := index.Query().IntRangeFilter("len", 1, 3).Exec()
	resLen1 := index.Query().IntRangeFilter("len", 1, 2).Exec()
	resA := index.Query().StringRangeFilter("k", "A", "B").Exec()

	resA := index.Query().IntInFilter("len", 1).Exec()
	resA := index.Query().StringInFilter("k", "A", "AA").Exec()

	resA := index.Query().StringInFilter("k", "A", "AA").IntRangeFilter("len", 1, 2).Exec()
