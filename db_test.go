package memdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
)

func TestDB_Outdated(t *testing.T) {
	db, err := OpenDB("", false)
	require.Nil(t, err)

	tx1 := db.Begin(true)
	err = tx1.Set("1", "abcde")
	require.Nil(t, err)

	err = tx1.Set("2", "ab")
	require.Nil(t, err)
	tx1.Commit()

	tx2 := db.Begin(true)
	assert.Nil(t, tx2.AddIndex(NewIndex("length", "*", func(a, b string) bool {
		return len(a) < len(b)
	})))

	// Nothing to clear
	db.cleanOutdated()

	got := make([]string, 0)
	assert.Nil(t, tx2.Ascend("length", func(key Key, value string) bool {
		got = append(got, value)
		return true
	}))

	assert.Equal(t, []string{"ab", "abcde"}, got)
	tx2.Commit()

	tx3 := db.Begin(true)
	err = tx3.Update("2", "aaaaaaaa")
	require.Nil(t, err)

	// Nothing to clear
	db.cleanOutdated()

	got = make([]string, 0)
	assert.Nil(t, tx3.Ascend("length", func(key Key, value string) bool {
		got = append(got, value)
		return true
	}))

	assert.Equal(t, []string{"abcde", "aaaaaaaa"}, got)
	tx3.Commit()

	got = make([]string, 0)
	for _, value := range db.items.get("2") {
		got = append(got, value.value)
	}
	assert.Equal(t, []string{"ab", "ab", "aaaaaaaa"}, got)

	// Need to clean one old record "ab"
	db.cleanOutdated()
	got = make([]string, 0)
	for _, value := range db.items.get("2") {
		got = append(got, value.value)
	}
	assert.Equal(t, []string{"ab", "aaaaaaaa"}, got)
}

func TestDB_FileImport(t *testing.T) {
	path := "test.db"
	os.RemoveAll(path)

	fs, err := OpenFileStorage(path)
	require.Nil(t, err)

	err = fs.Write([]*dbItem{
		{key: "FIRSTKEY", value: "FIRSTVALUE", createdTx: 1, deletedTx: 2},
		{key: "FIRSTKEY", value: "SECONDVALUE", createdTx: 2, deletedTx: 3},
		{key: "FIRSTKEY", value: "THIRDVALUE", createdTx: 3},
	}...)
	require.Nil(t, err)
	err = fs.Close()
	require.Nil(t, err)

	db, err := OpenDB("test.db", true)
	assert.Nil(t, err)

	items := db.items.get("FIRSTKEY")
	assert.Equal(t, []dbItem{
		{key: "FIRSTKEY", value: "FIRSTVALUE", createdTx: 1, deletedTx: 2},
		{key: "FIRSTKEY", value: "SECONDVALUE", createdTx: 2, deletedTx: 3},
		{key: "FIRSTKEY", value: "THIRDVALUE", createdTx: 3},
	}, items)

	tx := db.Begin(false)
	value, err := tx.Get("FIRSTKEY")
	assert.Nil(t, err)
	assert.Equal(t, "THIRDVALUE", value)
}
