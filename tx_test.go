package mvcc_attempt_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yddmat/mvcc_attempt"
)

func TestTransaction_Isolation(t *testing.T) {
	db := mvcc_attempt.NewDB()

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	tx1.Commit()

	tx1 = db.Begin(true)
	err = tx1.Delete("1")
	assert.Nil(t, err)

	r1, err := tx1.Get("1")
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
	assert.Equal(t, "", r1)

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Nil(t, err)
	assert.Equal(t, "first", r2)

	tx1.Commit()

	r2, err = tx2.Get("1")
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
	assert.Equal(t, "", r2)

	tx3 := db.Begin(false)
	r3, err := tx3.Get("1")
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
	assert.Equal(t, "", r3)

}

func TestTransaction_Set(t *testing.T) {
	db := mvcc_attempt.NewDB()

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	r1, err := tx1.Get("1")
	assert.Equal(t, "first", r1)

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
	assert.Empty(t, r2)

	tx1.Commit()
	r3, err := tx2.Get("1")
	assert.Equal(t, nil, err)
	assert.Equal(t, "first", r3)

	r4, err := tx1.Get("1")
	assert.Equal(t, mvcc_attempt.ErrTxClosed, err)
	assert.Empty(t, r4)
}

func TestTransaction_Rollback(t *testing.T) {
	db := mvcc_attempt.NewDB()

	tx1 := db.Begin(true)
	err := tx1.Set("1", "first")
	assert.Nil(t, err)
	tx1.Rollback()

	tx2 := db.Begin(false)
	r2, err := tx2.Get("1")
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
	assert.Empty(t, r2)
}

func TestTransaction_Delete(t *testing.T) {
	db := mvcc_attempt.NewDB()

	tx1 := db.Begin(false)
	err := tx1.Set("1", "first")
	assert.Equal(t, mvcc_attempt.ErrTxNotWritable, err)

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
	assert.Equal(t, mvcc_attempt.ErrNotFound, err)
}

func TestTransaction_Update(t *testing.T) {
	db := mvcc_attempt.NewDB()

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
	db := mvcc_attempt.NewDB()

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

	assert.Error(t, tx1.Ascend("test-len", func(key mvcc_attempt.Key, value string) bool {
		return true
	}))

	err = tx1.AddIndex(mvcc_attempt.NewIndex("test-len", "*", func(a, b string) bool {
		return len(a) < len(b)
	}))
	require.Nil(t, err)

	got := make([]mvcc_attempt.Key, 0)
	assert.Nil(t, tx1.Ascend("test-len", func(key mvcc_attempt.Key, value string) bool {
		got = append(got, key)
		return true
	}))

	assert.Equal(t, []mvcc_attempt.Key{"4", "2", "3", "5", "1"}, got)

	tx2 := db.Begin(false)
	assert.Error(t, tx2.Ascend("test-len", func(key mvcc_attempt.Key, value string) bool {
		return true
	}))
}
