package memdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func TestIndex_Insert(t *testing.T) {
	indexer := newIndexer()
	indexer.AddIndex(NewIndex("test-length", "*.test", func(a, b string) bool {
		return len(a) < len(b)
	}))

	indexer.Insert(&item{
		key: "1.test", value: "first",
	})

	indexer.Insert(&item{
		key: "2.test", value: "sec",
	})

	indexer.Insert(&item{
		key: "test", value: "wrong_key_value",
	})

	indexer.Insert(&item{
		key: "3.test", value: "thiiiird",
	})

	got := make([]*item, 0)

	indexer.GetIndex("test-length").tree.Ascend(func(i btree.Item) bool {
		item := i.(*item)
		got = append(got, item)
		return true
	})

	require.Equal(t, []*item{
		{key: "2.test", value: "sec"},
		{key: "1.test", value: "first"},
		{key: "3.test", value: "thiiiird"},
	}, got)
}

func TestIndex_Copy(t *testing.T) {
	indexer := newIndexer()

	indexer.AddIndex(NewIndex("test-length", "*.test", func(a, b string) bool {
		return len(a) < len(b)
	}))

	indexer.Insert(&item{
		key: "1.test", value: "first",
	})
	indexer.Insert(&item{
		key: "2.test", value: "sec",
	})

	newIndexer := indexer.Copy()

	store1 := make([]string, 0)
	indexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store1 = append(store1, i.(*item).value)
		return true
	})

	store2 := make([]string, 0)

	require.NotNil(t, newIndexer.storage["test-length"])
	newIndexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store2 = append(store2, i.(*item).value)
		return true
	})

	assert.Equal(t, store1, store2)

	newIndexer.Insert(&item{
		key: "3.test", value: "third",
	})

	store1 = make([]string, 0)
	indexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store1 = append(store1, i.(*item).value)
		return true
	})

	store2 = make([]string, 0)
	require.NotNil(t, newIndexer.storage["test-length"])
	newIndexer.storage["test-length"].tree.Ascend(func(i btree.Item) bool {
		store2 = append(store2, i.(*item).value)
		return true
	})

	assert.Equal(t, []string{"sec", "first"}, store1)
	assert.Equal(t, []string{"sec", "first", "third"}, store2)
}
