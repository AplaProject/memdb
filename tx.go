package memdb

import (
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/tidwall/btree"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrTxClosed      = errors.New("transaction closed")
	ErrTxNotWritable = errors.New("transaction is not writable")
)

type Transaction struct {
	id        uint64
	writable  bool
	operation uint64

	newIndexes *Indexes

	db *Database
}

func (tx *Transaction) Set(key Key, value string) error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	operation := atomic.AddUint64(&tx.operation, 1)

	_, err := tx.getLastKeyRevision(key, operation, tx.id)
	if err != ErrNotFound {
		return ErrAlreadyExists
	}

	item := &dbItem{key: key, createdTx: tx.id, createdOperation: operation, value: value}

	tx.db.items.set(key, item)
	tx.newIndexes.Insert(item)

	return nil
}

func (tx *Transaction) Get(key Key) (string, error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	operation := atomic.AddUint64(&tx.operation, 1)

	item, err := tx.getLastKeyRevision(key, operation, tx.id)
	if err != nil {
		return "", err
	}

	return item.value, nil
}

func (tx *Transaction) Delete(key Key) error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	operation := atomic.AddUint64(&tx.operation, 1)

	item, err := tx.getLastKeyRevision(key, operation, tx.id)
	if err != nil {
		return err
	}

	tx.newIndexes.Remove(&item)

	item.deletedTx = tx.id
	item.deletedOperation = operation

	tx.db.items.set(key, &item)

	return nil
}

func (tx *Transaction) Update(key Key, value string) error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	operation := atomic.AddUint64(&tx.operation, 1)

	item, err := tx.getLastKeyRevision(key, operation, tx.id)
	if err != nil {
		return err
	}

	item.deletedTx = tx.id
	item.deletedOperation = operation
	tx.db.items.set(key, &item)
	tx.newIndexes.Remove(&item)

	updItem := &dbItem{createdTx: tx.id, createdOperation: operation, value: value}
	tx.db.items.set(key, updItem)
	tx.newIndexes.Insert(&item)

	return nil
}

func (tx *Transaction) AddIndex(index *Index) error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	operation := atomic.AddUint64(&tx.operation, 1)

	err := tx.newIndexes.AddIndex(index)
	if err != nil {
		return err
	}

	for _, key := range tx.db.items.keys() {
		revision, err := tx.getLastKeyRevision(key, operation, tx.id)
		if err != nil {
			tx.newIndexes.RemoveIndex(index.name)
			return err
		}

		tx.newIndexes.Insert(&revision, index.name)
	}

	return nil
}

func (tx *Transaction) Ascend(index string, iterator func(key Key, value string) bool) error {
	if index == "" {
		return ErrEmptyIndex
	}

	indexes := tx.db.indexes
	if tx.writable {
		indexes = tx.newIndexes
	}

	i := indexes.GetIndex(index)
	if i == nil {
		return ErrUnknownIndex
	}

	var item *dbItem
	i.tree.Ascend(func(i btree.Item) bool {
		item = i.(*dbItem)
		return iterator(item.key, item.value)
	})

	return nil
}

func (tx *Transaction) Commit() error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if tx.writable {
		tx.db.indexes = tx.newIndexes
		tx.db.writeTx.Unlock()
	}

	tx.db.writers.set(tx.id, StatusDone)
	tx.db = nil

	return nil
}

func (tx *Transaction) Rollback() error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if tx.writable {
		tx.newIndexes = nil
	}

	tx.db.writers.set(tx.id, StatusRollback)
	tx.db = nil

	return nil
}

func (tx *Transaction) getLastKeyRevision(key Key, operation, txID uint64) (dbItem, error) {
	items := tx.db.items.get(key)
	var item dbItem
	for l := len(items) - 1; l >= 0; l-- {
		item = items[l]

		if txID == item.deletedTx {
			if operation > item.deletedOperation {
				return dbItem{}, ErrNotFound
			}

			return item, nil
		}

		if txID == item.createdTx {
			if operation > item.createdOperation {
				return item, nil
			}

			continue
		}

		if txID > item.deletedTx {
			if tx.db.writers.get(item.deletedTx) == StatusDone {
				return dbItem{}, ErrNotFound
			}

			if tx.db.writers.get(item.createdTx) == StatusDone {
				return item, nil
			}

			continue
		}
	}

	return dbItem{}, ErrNotFound
}
