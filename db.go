package mvcc_attempt

import (
	"sync"
	"sync/atomic"
)

type Key string

type dbItem struct {
	value string

	createdTx uint64
	deletedTx uint64

	createdOperation uint64
	deletedOperation uint64
}

type Items struct {
	items map[Key][]dbItem
	mu    sync.RWMutex
}

func (it *Items) set(key Key, item dbItem) {
	it.mu.Lock()
	it.items[key] = append(it.items[key], item)
	it.mu.Unlock()
}

func (it *Items) get(key Key) []dbItem {
	it.mu.RLock()
	defer it.mu.RUnlock()

	itemsCopy := make([]dbItem, len(it.items[key]))
	copy(itemsCopy, it.items[key])

	return itemsCopy
}

type Database struct {
	writeMu sync.Mutex

	storage Items

	writers TxsStatus
	lastTx  uint64
}

func NewDB() *Database {
	return &Database{
		storage: Items{items: make(map[Key][]dbItem)},
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
