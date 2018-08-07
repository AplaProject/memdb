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

type Indexes struct {
	mu      sync.RWMutex
	storage map[string]*Index
}

func newIndexer() *Indexes {
	return &Indexes{
		storage: make(map[string]*Index),
	}
}

func (idxer *Indexes) addIndex(index *Index) error {
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

func (idxer *Indexes) removeIndex(name string) error {
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

func (idxer *Indexes) insert(item *dbItem, to ...string) {
	var all bool
	if len(to) == 0 {
		all = true
	}

	in := func(slice []string, value string) bool {
		for _, v := range slice {
			if v == value {
				return true
			}
		}

		return false
	}

	idxer.mu.Lock()
	for _, index := range idxer.storage {
		if (all || in(to, index.name)) && match.Match(string(item.key), index.pattern) {
			index.tree.ReplaceOrInsert(item)
		}
	}
	idxer.mu.Unlock()
}

func (idxer *Indexes) copy() *Indexes {
	newIndexer := newIndexer()

	idxer.mu.RLock()
	for _, oldIdx := range idxer.storage {
		newIdx := NewIndex(oldIdx.name, oldIdx.pattern, oldIdx.sortFn)
		newIdx.tree = oldIdx.tree.Clone()

		err := newIndexer.addIndex(newIdx)
		if err != nil {
			panic(err)
		}
	}
	idxer.mu.RUnlock()

	return newIndexer
}
