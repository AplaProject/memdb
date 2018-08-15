package memdb

import (
	"sync"

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
	writable bool

	db           *Database
	newIndexes   *Indexes
	pendingItems map[Key]struct{}
	mu           sync.RWMutex
}

func (tx *Transaction) Set(key Key, value string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	_, err := tx.getKey(key)
	if err != ErrNotFound {
		return ErrAlreadyExists
	}

	new := &item{key: key, value: value}
	tx.createItem(new)
	tx.newIndexes.Insert(new)

	return nil
}

func (tx *Transaction) Get(key Key) (string, error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}

	item, err := tx.getKey(key)
	if err != nil {
		return "", err
	}

	return item.value, nil
}

func (tx *Transaction) Delete(key Key) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	item, err := tx.getKey(key)
	if err != nil {
		return err
	}

	tx.updateItem(key, nil, true)
	tx.newIndexes.Remove(&item)

	return nil
}

func (tx *Transaction) Update(key Key, value string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	_, err := tx.getKey(key)
	if err != nil {
		return err
	}

	update := &item{key: key, value: value}
	tx.updateItem(key, update, false)
	tx.newIndexes.Insert(update)

	return nil
}

func (tx *Transaction) AddIndex(index *Index) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	if !tx.writable {
		return ErrTxNotWritable
	}

	err := tx.newIndexes.AddIndex(index)
	if err != nil {
		return err
	}

	for _, key := range tx.db.items.keys() {
		revision, err := tx.getKey(key)
		if err != nil {
			tx.newIndexes.RemoveIndex(index.name)
			return err
		}

		tx.newIndexes.Insert(&revision, index.name)
	}

	return nil
}

func (tx *Transaction) Ascend(index string, iterator func(key Key, value string) bool) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return ErrTxClosed
	}

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

	var curitem *item
	i.tree.Ascend(func(bitem btree.Item) bool {
		curitem = bitem.(*item)
		return iterator(curitem.key, curitem.value)
	})

	return nil
}

func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	db := tx.db
	tx.db = nil

	if tx.writable {
		save := make([]fileItem, 0)

		for key := range tx.pendingItems {
			dbItem := db.items.get(key)
			dbItem.Lock()

			if dbItem.pendingDeleted {
				if dbItem.current != nil {
					save = append(save, fileItem{item: item{key: key}, command: commandDEL})
				}
				dbItem.Unlock()
				db.items.remove(key)
				continue
			}

			// Delete old record
			if dbItem.current != nil {
				save = append(save, fileItem{item: item{key: key}, command: commandDEL})
			}

			save = append(save, fileItem{item: item{key: key, value: dbItem.pending.value}, command: commandSET})
			dbItem.current = dbItem.pending
			dbItem.pending = nil
			dbItem.Unlock()
		}

		tx.pendingItems = nil
		db.indexes = tx.newIndexes

		// Write to disk
		if db.persist {
			err := db.persistentStorage.write(save...)
			if err != nil {
				return err
			}
		}

		db.writeTx.Unlock()
	}

	return nil
}

func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	db := tx.db

	if tx.writable {
		tx.newIndexes = nil
		tx.pendingItems = nil
		db.writeTx.Unlock()
	}

	return nil
}

func (tx *Transaction) getKey(key Key) (item, error) {
	dbItem := tx.db.items.get(key)

	if dbItem == nil {
		return item{}, ErrNotFound
	}

	dbItem.RLock()
	defer dbItem.RUnlock()

	// Item doesn't created "yet"
	if !tx.writable && dbItem.current == nil || (tx.writable && dbItem.pendingDeleted) {
		return item{}, ErrNotFound
	}

	// Item was already updated at this transaction
	if tx.writable && !dbItem.pendingDeleted && dbItem.pending != nil {
		return *dbItem.pending, nil
	}

	return *dbItem.current, nil
}

func (tx *Transaction) createItem(item *item) {
	dbItem := &dbItem{key: item.key, pending: item}

	dbItem.Lock()
	tx.db.items.set(item.key, dbItem)
	tx.pendingItems[dbItem.key] = struct{}{}
	dbItem.Unlock()
}

func (tx *Transaction) updateItem(key Key, new *item, deleted bool) {
	dbItem := tx.db.items.get(key)

	dbItem.Lock()
	dbItem.pendingDeleted = deleted
	dbItem.pending = new
	tx.pendingItems[key] = struct{}{}
	dbItem.Unlock()
}
