package memdb

import (
	"sync"
	"sync/atomic"

	"github.com/tidwall/btree"
)

type Key string

type dbItem struct {
	key   Key
	value string

	createdTx uint64
	deletedTx uint64

	createdOperation uint64
	deletedOperation uint64
}

func (i *dbItem) Less(item btree.Item, ctx interface{}) bool {
	i2 := item.(*dbItem)
	index, ok := ctx.(*Index)
	if ok {
		if index.sortFn != nil {
			// Using an Index
			if index.sortFn(i.value, i2.value) {
				return true
			}
			if index.sortFn(i2.value, i.value) {
				return false
			}
		}
	}

	return i.key < i2.key
}

type Items struct {
	mu      sync.RWMutex
	storage map[Key][]*dbItem
}

func (it *Items) set(key Key, item *dbItem) {
	it.mu.Lock()
	it.storage[key] = append(it.storage[key], item)
	it.mu.Unlock()
}

func (it *Items) get(key Key) []dbItem {
	it.mu.RLock()
	defer it.mu.RUnlock()

	itemsCopy := make([]dbItem, len(it.storage[key]))
	for _, item := range it.storage[key] {
		itemsCopy = append(itemsCopy, *item)
	}

	return itemsCopy
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

	writers txsStatus
	lastTx  uint64
}

func NewDB() *Database {
	return &Database{
		items:   Items{storage: make(map[Key][]*dbItem)},
		indexes: newIndexer(),
		writers: txsStatus{txs: make(map[uint64]Status)},
	}
}

func (db *Database) Begin(writable bool) *Transaction {
	txID := atomic.AddUint64(&db.lastTx, 1)

	tx := &Transaction{
		id: txID,
		db: db,
	}

	if writable {
		db.writeTx.Lock()
		tx.writable = true
		tx.newIndexes = db.indexes.copy()
	}

	db.writers.set(txID, StatusRunning)

	return tx
}

type Status int8

const (
	StatusUnknown Status = iota
	StatusRunning
	StatusDone
	StatusRollback
)

// txsStatus is storing current writing transactions state
type txsStatus struct {
	txs map[uint64]Status
	mu  sync.RWMutex
}

func (atx *txsStatus) get(tx uint64) Status {
	atx.mu.RLock()
	defer atx.mu.RUnlock()
	return atx.txs[tx]
}

func (atx *txsStatus) set(tx uint64, status Status) {
	atx.mu.Lock()
	defer atx.mu.Unlock()
	atx.txs[tx] = status
}
