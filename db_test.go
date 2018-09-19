package memdb

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_FileImport(t *testing.T) {
	path := "test.db"
	os.RemoveAll(path)

	fs, err := openFileStorage(path)
	require.Nil(t, err)

	err = fs.write([]fileItem{
		{item: item{key: "FIRSTKEY", value: "FIRSTVALUE"}, command: commandSET},
		{item: item{key: "FIRSTKEY"}, command: commandDEL},
		{item: item{key: "FIRSTKEY", value: "THIRDVALUE"}, command: commandSET},
	}...)

	require.Nil(t, err)
	err = fs.close()
	require.Nil(t, err)

	db, err := OpenDB("test.db", true)
	assert.Nil(t, err)

	last := db.items.get("FIRSTKEY")
	assert.Equal(t, "THIRDVALUE", last.current.value)

	tx := db.Begin(false)
	value, err := tx.Get("FIRSTKEY")
	assert.Nil(t, err)
	assert.Equal(t, "THIRDVALUE", value)
}

func BenchmarkDatabaseSet(b *testing.B) {
	b.ReportAllocs()
	require.Nil(b, os.RemoveAll("bench.db"))
	db, err := OpenDB("bench.db", false)
	require.Nil(b, err)

	b.ResetTimer()
	tx := db.Begin(true)
	for n := 0; n < b.N; n++ {
		err = tx.Set(strconv.FormatInt(int64(n), 10), "somevalue")
		if err != nil {
			require.Nil(b, err)
		}
	}

	if err := tx.Commit(); err != nil {
		require.Nil(b, err)
	}
}
