package memdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestIndex_Insert(t *testing.T) {
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

func TestIndex_Copy(t *testing.T) {
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

	newIndexer := indexer.copy()

	store1 := make([]string, 0)
	indexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store1 = append(store1, i.(*dbItem).value)
		return true
	})

	store2 := make([]string, 0)

	require.NotNil(t, newIndexer.storage["test-length"])
	newIndexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store2 = append(store2, i.(*dbItem).value)
		return true
	})

	assert.Equal(t, store1, store2)

	newIndexer.insert(&dbItem{
		key: "3.test", value: "third",
	})

	store1 = make([]string, 0)
	indexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store1 = append(store1, i.(*dbItem).value)
		return true
	})

	store2 = make([]string, 0)
	require.NotNil(t, newIndexer.storage["test-length"])
	newIndexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store2 = append(store2, i.(*dbItem).value)
		return true
	})

	assert.Equal(t, []string{"sec", "first"}, store1)
	assert.Equal(t, []string{"sec", "first", "third"}, store2)
}
