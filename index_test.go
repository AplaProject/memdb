package memdb

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
	"github.com/tidwall/buntdb"
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

func TestIndex_MultipleIndex(t *testing.T) {
	indexer := newIndexer()
	indexer.AddIndex(NewIndex("multiple", "*", buntdb.IndexJSON("name.last"), buntdb.IndexJSON("age")))

	cases := []string{
		`{"name":{"first":"Tom","last":"Johnson"},"age":38}`,
		`{"name":{"first":"Janet","last":"Prichard"},"age":47}`,
		`{"name":{"first":"Carol","last":"Anderson"},"age":52}`,
		`{"name":{"first":"Alan","last":"Cooper"},"age":28}`,
		`{"name":{"first":"Sam","last":"Anderson"},"age":51}`,
		`{"name":{"first":"Melinda","last":"Prichard"},"age":44}`,
	}

	for key, c := range cases {
		indexer.Insert(&item{
			key:   dbKey(strconv.FormatInt(int64(key), 10)),
			value: c,
		})
	}

	got := make([]string, 0)
	indexer.storage["multiple"].tree.Ascend(func(i btree.Item) bool {
		got = append(got, i.(*item).value)
		return true
	})

	require.Equal(t, []string{
		cases[4], cases[2], cases[3], cases[0], cases[5], cases[1],
	}, got)
}
