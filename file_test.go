package memdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileStorage_ReadWrite(t *testing.T) {
	os.RemoveAll("test.db")
	fs, err := openFileStorage("test.db")
	assert.Nil(t, err)

	err = fs.write([]fileItem{
		{item: item{key: "1", value: "test1"}, command: commandSET},
		{item: item{key: "2"}, command: commandDEL},
		{item: item{key: "3", value: "test3"}, command: commandSET},
	}...)
	assert.Nil(t, err)

	fs.close()

	fs, err = openFileStorage("test.db")
	assert.Nil(t, err)

	items := fs.read()
	got := make([]fileItem, 0)
	for result := range items {
		require.Nil(t, result.err)
		got = append(got, result.item)
	}

	assert.Equal(t, []fileItem{
		{command: commandSET, item: item{key: "1", value: "test1"}},
		{command: commandDEL, item: item{key: "2"}},
		{command: commandSET, item: item{key: "3", value: "test3"}},
	}, got)
}
