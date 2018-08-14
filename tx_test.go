package memdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransaction_Isolation(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	tx1.Commit()

	tx1 = db.Begin(true)
	err = tx1.Delete("1")
	assert.Nil(t, err)

	r1, err := tx1.Get("1")
	assert.Equal(t, ErrNotFound, err)
	assert.Equal(t, "", r1)

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "first", r2)

	tx1.Commit()

	r2, err = tx2.Get("1")
	assert.Equal(t, ErrNotFound, err)
	assert.Equal(t, "", r2)

	tx3 := db.Begin(false)
	r3, err := tx3.Get("1")
	assert.Equal(t, ErrNotFound, err)
	assert.Equal(t, "", r3)
}

func TestTransaction_Set(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	r1, err := tx1.Get("1")
	assert.Equal(t, "first", r1)

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Equal(t, ErrNotFound, err)
	assert.Empty(t, r2)

	tx1.Commit()
	r3, err := tx2.Get("1")
	assert.Equal(t, nil, err)
	assert.Equal(t, "first", r3)

	r4, err := tx1.Get("1")
	assert.Equal(t, ErrTxClosed, err)
	assert.Empty(t, r4)
}

func TestTransaction_SetPersistent(t *testing.T) {
	os.RemoveAll("test.db")
	db, _ := OpenDB("test.db", true)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	r1, err := tx1.Get("1")
	assert.Equal(t, "first", r1)
	assert.Nil(t, tx1.Commit())
	assert.Nil(t, db.Close())

	fs, err := openFileStorage("test.db")
	assert.Nil(t, err)

	got := make([]item, 0)
	for item := range fs.read() {
		assert.Nil(t, item.err)
		got = append(got, item.item.item)
	}

	assert.Len(t, got, 1)
	assert.Equal(t, "first", got[0].value)
	assert.Equal(t, Key("1"), got[0].key)
}

func TestTransaction_SetAlreadyExists(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)

	err = tx1.Set("1", "first")
	assert.Equal(t, ErrAlreadyExists, err)
}

func TestTransaction_Rollback(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	tx1.Rollback()

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Equal(t, ErrNotFound, err)
	assert.Empty(t, r2)
}

func TestTransaction_Delete(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(false)
	err := tx1.Set("1", "first")
	assert.Equal(t, ErrTxNotWritable, err)

	tx2 := db.Begin(true)
	err = tx2.Set("1", "first")
	assert.Nil(t, err)
	tx2.Commit()

	tx3 := db.Begin(true)
	err = tx3.Delete("1")
	assert.Nil(t, err)
	tx3.Commit()

	tx4 := db.Begin(false)
	r4, err := tx4.Get("1")
	assert.Empty(t, r4)
	assert.Equal(t, ErrNotFound, err)
}

func TestTransaction_DeleteNonExistent(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Delete("1")
	assert.Equal(t, ErrNotFound, err)
}

func TestTransaction_Update(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	tx1.Commit()

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "first", r2)

	tx3 := db.Begin(true)
	tx3.Update("1", "second")
	r3, err := tx2.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "first", r3)

	r4, err := tx3.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "second", r4)
	tx3.Commit()

	r5, err := tx2.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "second", r5)
}

func TestTransaction_AddIndex(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "abcde")
	require.Nil(t, err)
	err = tx1.Set("2", "ab")
	require.Nil(t, err)
	err = tx1.Set("3", "abc")
	require.Nil(t, err)
	err = tx1.Set("4", "a")
	require.Nil(t, err)
	err = tx1.Set("5", "abcd")
	require.Nil(t, err)

	assert.Error(t, tx1.Ascend("test-len", func(key Key, value string) bool {
		return true
	}))

	err = tx1.AddIndex(NewIndex("test-len", "*", func(a, b string) bool {
		return len(a) < len(b)
	}))
	require.Nil(t, err)

	got := make([]Key, 0)
	assert.Nil(t, tx1.Ascend("test-len", func(key Key, value string) bool {
		got = append(got, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1"}, got)

	tx2 := db.Begin(false)
	assert.Error(t, tx2.Ascend("test-len", func(key Key, value string) bool {
		return true
	}))
}

func TestTransaction_IndexIsolation(t *testing.T) {
	db, _ := OpenDB("", false)

	tx1 := db.Begin(true)
	err := tx1.Set("1", "abcde")
	require.Nil(t, err)
	err = tx1.Set("2", "ab")
	require.Nil(t, err)
	err = tx1.Set("3", "abc")
	require.Nil(t, err)
	err = tx1.Set("4", "a")
	require.Nil(t, err)
	err = tx1.Set("5", "abcd")
	require.Nil(t, err)

	assert.Error(t, tx1.Ascend("test-len", func(key Key, value string) bool {
		return true
	}))

	err = tx1.AddIndex(NewIndex("test-len", "*", func(a, b string) bool {
		return len(a) < len(b)
	}))
	require.Nil(t, err)
	tx1.Commit()

	tx2 := db.Begin(false)
	tx1 = db.Begin(true)

	got1 := make([]Key, 0)
	assert.Nil(t, tx1.Ascend("test-len", func(key Key, value string) bool {
		got1 = append(got1, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1"}, got1)

	got2 := make([]Key, 0)
	assert.Nil(t, tx2.Ascend("test-len", func(key Key, value string) bool {
		got2 = append(got2, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1"}, got2)

	err = tx1.Set("6", "abcdef")
	require.Nil(t, err)

	got1 = make([]Key, 0)
	assert.Nil(t, tx1.Ascend("test-len", func(key Key, value string) bool {
		got1 = append(got1, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1", "6"}, got1)

	got2 = make([]Key, 0)
	assert.Nil(t, tx2.Ascend("test-len", func(key Key, value string) bool {
		got2 = append(got2, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1"}, got2)

	tx1.Commit()

	got2 = make([]Key, 0)
	assert.Nil(t, tx2.Ascend("test-len", func(key Key, value string) bool {
		got2 = append(got2, key)
		return true
	}))

	assert.Equal(t, []Key{"4", "2", "3", "5", "1", "6"}, got2)
}
