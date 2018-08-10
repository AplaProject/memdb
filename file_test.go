package memdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStorage_ReadWrite(t *testing.T) {
	os.RemoveAll("test.db")
	fs, err := OpenFileStorage("test.db")
	assert.Nil(t, err)

	err = fs.Write([]*dbItem{{key: "1", value: "test1"}, {key: "2", value: "test2"}, {key: "3", value: "test3"}}...)
	assert.Nil(t, err)

	fs.Close()

	fs, err = OpenFileStorage("test.db")
	assert.Nil(t, err)

	items := fs.Read()
	got := make([]rowItem, 0)
	for result := range items {
		require.Nil(t, result.err)
		got = append(got, result.item)
	}

	assert.Equal(t, []rowItem{
		{command: CommandSET, dbItem: dbItem{key: "1", value: "test1"}},
		{command: CommandSET, dbItem: dbItem{key: "2", value: "test2"}},
		{command: CommandSET, dbItem: dbItem{key: "3", value: "test3"}},
	}, got)
}