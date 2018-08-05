package mvcc_attempt

import (
	"errors"

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

// Indexes is not safe for concurrent use
type Indexes struct {
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

	idxer.storage[index.name] = index

	return nil
}

func (idxer *Indexes) removeIndex(name string) error {
	if name == "" {
		return ErrEmptyIndex
	}

	delete(idxer.storage, name)
	return nil
}

func (idxer *Indexes) GetIndex(name string) *Index {
	for indexName, index := range idxer.storage {
		if name == indexName {
			return index
		}
	}
	return nil
}

func (idxer *Indexes) insert(item *dbItem, to ...string) {
	in := func(slice []string, value string) bool {
		if len(slice) == 0 {
			return true
		}

		for _, v := range slice {
			if v == value {
				return true
			}
		}

		return false
	}

	for _, index := range idxer.storage {
		if in(to, index.name) && match.Match(string(item.key), index.pattern) {
			index.tree.ReplaceOrInsert(item)
		}
	}
}

// TODO
func (idxer *Indexes) copy() *Indexes {
	return &Indexes{}
}
