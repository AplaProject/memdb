package mvcc_attempt

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
	items map[Key][]*dbItem
	mu    sync.RWMutex
}

func (it *Items) set(key Key, item *dbItem) {
	it.mu.Lock()
	it.items[key] = append(it.items[key], item)
	it.mu.Unlock()
}

func (it *Items) get(key Key) []dbItem {
	it.mu.RLock()
	defer it.mu.RUnlock()

	itemsCopy := make([]dbItem, len(it.items[key]))
	for _, item := range it.items[key] {
		itemsCopy = append(itemsCopy, *item)
	}

	return itemsCopy
}

type Database struct {
	writeMu sync.Mutex

	storage Items
	indexes *Indexes

	writers TxsStatus
	lastTx  uint64
}

func NewDB() *Database {
	return &Database{
		storage: Items{items: make(map[Key][]*dbItem)},
		indexes: newIndexer(),
		writers: TxsStatus{storage: make(map[uint64]Status)},
	}
}

func (db *Database) Begin(writable bool) *Transaction {
	txID := atomic.AddUint64(&db.lastTx, 1)

	tx := &Transaction{
		id: txID,
		db: db,
	}

	if writable {
		db.writeMu.Lock()
		tx.writable = true
		tx.newIndexes = db.indexes.copy()
	}

	return tx
}

type Status int8

const (
	StatusUnknown Status = iota
	StatusDone
	StatusRollback
)

type TxsStatus struct {
	storage map[uint64]Status
	mu      sync.RWMutex
}

func (atx *TxsStatus) Get(tx uint64) Status {
	atx.mu.RLock()
	defer atx.mu.RUnlock()
	return atx.storage[tx]
}

func (atx *TxsStatus) set(tx uint64, status Status) {
	atx.mu.Lock()
	defer atx.mu.Unlock()
	atx.storage[tx] = status
}
