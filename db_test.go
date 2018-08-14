package memdb

import (
	"os"
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
