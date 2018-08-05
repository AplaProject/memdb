package mvcc_attempt

import (
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/tidwall/btree"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrTxClosed      = errors.New("transaction closed")
	ErrTxNotWritable = errors.New("transaction is not writable")
)

type Transaction struct {
	id        uint64
	writable  bool
	operation uint64

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
	item := &dbItem{key: key, createdTx: tx.id, createdOperation: operation, value: value}

	tx.db.storage.set(key, item)
	tx.db.indexes.insert(item)

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

	item.deletedTx = tx.id
	item.deletedOperation = operation

	tx.db.storage.set(key, &item)

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
	tx.db.storage.set(key, &item)

	updItem := &dbItem{createdTx: tx.id, createdOperation: operation, value: value}
	tx.db.storage.set(key, updItem)

	return nil
}

func (tx *Transaction) AddIndex(indexes ...*Index) error {
	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	operation := atomic.AddUint64(&tx.operation, 1)

	for _, index := range indexes {
		err := tx.db.indexes.addIndex(index)
		if err != nil {
			return err
		}

		tx.db.storage.mu.RLock()
		tx.db.storage.mu.RUnlock()
		for key, _ := range tx.db.storage.items {
			revision, err := tx.getLastKeyRevision(key, operation, tx.id)
			if err != nil {
				tx.db.indexes.removeIndex(index.name)
				return err
			}

			tx.db.indexes.insert(&revision, index.name)
		}
	}

	return nil
}

func (tx *Transaction) Ascend(index string, iterator func(key Key, value string) bool) error {
	if index == "" {
		return ErrEmptyIndex
	}

	indexes := tx.db.roIndexes
	if tx.writable {
		indexes = tx.db.indexes
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

func (tx *Transaction) getLastKeyRevision(key Key, operation, txID uint64) (dbItem, error) {
	items := tx.db.storage.get(key)
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
			if tx.db.writers.Get(item.deletedTx) == StatusDone {
				return dbItem{}, ErrNotFound
			}

			if tx.db.writers.Get(item.createdTx) == StatusDone {
				return item, nil
			}

			continue
		}
	}

	return dbItem{}, ErrNotFound
}

func (tx *Transaction) Commit() {
	if tx.writable {
		tx.db.writers.set(tx.id, StatusDone)
		tx.db.writeMu.Unlock()
	}

	tx.db = nil
}

func (tx *Transaction) Rollback() {
	if tx.writable && tx.db.writers.Get(tx.id) != StatusDone {
		tx.db.writers.set(tx.id, StatusRollback)
	}

	tx.db = nil
}
