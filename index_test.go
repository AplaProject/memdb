package mvcc_attempt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestIndexInsert(t *testing.T) {
	indexer := newIndexer()
	indexer.addIndex(NewIndex("test-length", "*.test", func(a, b string) bool {
		return len(a) < len(b)
	}))

	indexer.insert(&dbItem{
		key: "1.test", value: "first",
	})

	indexer.insert(&dbItem{
		key: "2.test", value: "sec",
	})

	indexer.insert(&dbItem{
		key: "test", value: "wrong_key_value",
	})

	indexer.insert(&dbItem{
		key: "3.test", value: "thiiiird",
	})

	got := make([]*dbItem, 0)

	indexer.GetIndex("test-length").tree.Ascend(func(i btree.Item) bool {
		item := i.(*dbItem)
		got = append(got, item)
		return true
	})

	require.Equal(t, []*dbItem{
		{key: "2.test", value: "sec"},
		{key: "1.test", value: "first"},
		{key: "3.test", value: "thiiiird"},
	}, got)
}
