package memdb

import (
	"errors"
	"sync"

	"github.com/tidwall/btree"
	"github.com/tidwall/match"
)

const btreeDegrees = 64

var (
	ErrEmptyIndex   = errors.New("index name is empty")
	ErrIndexExists  = errors.New("index already exists")
	ErrUnknownIndex = errors.New("unknown index")
)

type Index struct {
	name    string
	pattern string
	tree    *btree.BTree
	sortFn  func(a, b string) bool
}

func NewIndex(name, pattern string, sortFn func(a, b string) bool) *Index {
	i := new(Index)
	i.tree = btree.New(btreeDegrees, i)
	i.pattern = pattern
	i.name = name
	i.sortFn = sortFn
	return i
}

func (idx *Index) insert(item btree.Item) {
	idx.tree.ReplaceOrInsert(item)
}

func (idx *Index) remove(item btree.Item) {
	idx.tree.Delete(item)
}

type Indexes struct {
	mu      sync.RWMutex
	storage map[string]*Index
}

func newIndexer() *Indexes {
	return &Indexes{
		storage: make(map[string]*Index),
	}
}

func (idxer *Indexes) AddIndex(index *Index) error {
	if index.name == "" {
		return ErrEmptyIndex
	}

	if _, ok := idxer.storage[index.name]; ok {
		return ErrIndexExists
	}

	idxer.mu.Lock()
	idxer.storage[index.name] = index
	idxer.mu.Unlock()

	return nil
}

func (idxer *Indexes) RemoveIndex(name string) error {
	if name == "" {
		return ErrEmptyIndex
	}

	idxer.mu.Lock()
	delete(idxer.storage, name)
	idxer.mu.Unlock()

	return nil
}

func (idxer *Indexes) GetIndex(name string) *Index {
	idxer.mu.RLock()
	defer idxer.mu.RUnlock()

	for indexName, index := range idxer.storage {
		if name == indexName {
			return index
		}
	}

	return nil
}

func (idxer *Indexes) Insert(item *dbItem, to ...string) {
	idxer.mu.Lock()
	for _, index := range idxer.storage {
		if idxer.fit(index.name, to) && match.Match(string(item.key), index.pattern) {
			index.insert(item)
		}
	}
	idxer.mu.Unlock()
}

func (idxer *Indexes) Remove(item *dbItem, from ...string) {
	idxer.mu.Lock()
	for _, index := range idxer.storage {
		if idxer.fit(index.name, from) && match.Match(string(item.key), index.pattern) {
			index.remove(item)
		}
	}
	idxer.mu.Unlock()
}

func (idxer *Indexes) fit(current string, indexes []string) bool {
	if len(indexes) == 0 {
		return true
	}

	for _, v := range indexes {
		if v == current {
			return true
		}
	}

	return false
}

func (idxer *Indexes) Copy() *Indexes {
	newIndexer := newIndexer()

	idxer.mu.RLock()
	for _, oldIdx := range idxer.storage {
		newIdx := NewIndex(oldIdx.name, oldIdx.pattern, oldIdx.sortFn)
		newIdx.tree = oldIdx.tree.Clone()

		err := newIndexer.AddIndex(newIdx)
		if err != nil {
			panic(err)
		}
	}
	idxer.mu.RUnlock()

	return newIndexer
}
