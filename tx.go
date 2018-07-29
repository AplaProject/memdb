package mvcc_attempt

import (
	"sync/atomic"

	"github.com/pkg/errors"
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
	item := dbItem{createdTx: tx.id, createdOperation: operation, value: value}

	tx.db.storage.set(key, item)
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

	tx.db.storage.set(key, item)

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
	tx.db.storage.set(key, item)

	item = dbItem{createdTx: tx.id, createdOperation: operation, value: value}
	tx.db.storage.set(key, item)

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
