package memdb

import (
	"sync"

	"github.com/tidwall/btree"
)

type Key string

type item struct {
	key   Key
	value string
}

type dbItem struct {
	key            Key
	current        *item
	pending        *item
	pendingDeleted bool
	sync.RWMutex
}

func (i *item) Less(bitem btree.Item, ctx interface{}) bool {
	i2 := bitem.(*item)
	index, ok := ctx.(*Index)
	if ok {
		if index.sortFn(i.value, i2.value) {
			return true
		}
		if index.sortFn(i2.value, i.value) {
			return false
		}
	}

	return i.key < i2.key
}

type Items struct {
	mu      sync.RWMutex
	storage map[Key]*dbItem
}

func (it *Items) set(key Key, item *dbItem) {
	it.mu.Lock()
	it.storage[key] = item
	it.mu.Unlock()
}

func (it *Items) get(key Key) *dbItem {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.storage[key]
}

func (it *Items) remove(key Key) {
	it.mu.Lock()
	delete(it.storage, key)
	it.mu.Unlock()
}

func (it *Items) keys() []Key {
	keys := make([]Key, 0)

	it.mu.RLock()
	for key := range it.storage {
		keys = append(keys, key)
	}

	it.mu.RUnlock()
	return keys
}

type Database struct {
	writeTx sync.Mutex

	items   Items
	indexes *Indexes

	closed            bool
	persist           bool
	persistentStorage *fileStorage
}

func OpenDB(path string, persist bool) (*Database, error) {
	db := &Database{
		items:   Items{storage: make(map[Key]*dbItem)},
		indexes: newIndexer(),
	}

	if persist {
		var err error
		db.persist = true
		db.persistentStorage, err = openFileStorage(path)
		if err != nil {
			return nil, err
		}

		records := db.persistentStorage.read()
		for record := range records {
			if record.err != nil {
				return nil, err
			}

			if record.item.command == commandSET {
				dbItem := &dbItem{key: record.item.key, current: &record.item.item}
				db.items.set(dbItem.key, dbItem)
			}

			if record.item.command == commandDEL {
				db.items.remove(record.item.key)
			}
		}
	}

	return db, nil
}

func (db *Database) Close() error {
	if db.closed {
		return nil
	}

	err := db.persistentStorage.close()
	if err != nil {
		return err
	}

	db.closed = true
	db.items = Items{storage: make(map[Key]*dbItem)}
	db.indexes = newIndexer()

	return nil
}

func (db *Database) Begin(writable bool) *Transaction {
	tx := &Transaction{
		db: db,
	}

	if writable {
		db.writeTx.Lock()
		tx.writable = true
		tx.pendingItems = make(map[Key]struct{})
		tx.newIndexes = db.indexes.Copy()
	}

	return tx
}
