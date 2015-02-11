# GoIndex
In-memory document search


### Example ###

    import "github.com/hamaxx/goindex"

    index := goindex.New()

	index.NewDoc("A").IntKey("len", 1).StringKey("k", "A")
	index.NewDoc("B").IntKey("len", 1).StringKey("k", "B")
	index.NewDoc("AA").IntKey("len", 2).StringKey("k", "AA")

	resFull := index.Query().IntFilter("len", 1, 3).Exec()
	resLen1 := index.Query().IntFilter("len", 1, 2).Exec()
	resA := index.Query().StringFilter("k", "A", "B").Exec()
