package bptree

import (
	"os"
	"sync"
)

type Tree struct {
	sync.Mutex
	Root       Item
	Mid        int
	file       *os.File
	pagesCount int32
	keySize    int
	dataSize   int
	savechan   chan *Leaf
	close      bool
}

func (tree *Tree) Init(keySize, dataSize int) {
	tree.keySize = keySize
	tree.dataSize = dataSize
}

func (tree *Tree) createLeaf() *Leaf {
	tree.pagesCount++

	return &Leaf{
		tree:   tree,
		page:   tree.pagesCount - 1,
		rows:   make(Rows, tree.Mid*2),
		loaded: true,
	}
}

func (tree *Tree) createBranch() *Branch {
	return &Branch{tree: tree}
}

func (tree *Tree) Insert(row *Row) {
	if tree.Root == nil {
		tree.Root = tree.createLeaf()
	}

	tree.Root.Insert(row)

	if tree.Root.IsFull() {
		branch := tree.createBranch()
		branch.items = Items{tree.Root}
		tree.Root.split(branch)
		tree.Root = branch
	} else {
		if leaf, ok := tree.Root.(*Leaf); ok {
			leaf.Save()
		}
	}
}

func (tree *Tree) Find(key Key) *Row {
	if tree.Root == nil {
		return nil
	}

	return tree.Root.Find(key)
}

func (tree *Tree) GetPageInfoSize() int {
	return 3 * 5 // index, next, count * 5 bytes
}

func (tree *Tree) GetKeySize() int {
	return tree.keySize
}

func (tree *Tree) GetDataSize() int {
	return tree.dataSize
}

func (tree *Tree) GetRowSize() int {
	return 8 + tree.keySize + tree.dataSize
}

func (tree *Tree) GetPageSize() int {
	return tree.GetPageInfoSize() + tree.GetRowSize()*((2*tree.Mid)-1)
}

func (tree *Tree) GetHeight() int {
	result := 0
	if tree.Root != nil {
		item := tree.Root
		for {
			result++
			if item.IsLeaf() {
				break
			}

			if i, ok := item.(*Branch); ok {
				item = i.items[0]
			}
		}
	}
	return result
}

func (tree *Tree) FirstLeaf() (leaf *Leaf) {
	if tree.Root != nil {
		item := tree.Root
		for {
			if item.IsLeaf() {
				return item.(*Leaf)
			}

			if i, ok := item.(*Branch); ok {
				item = i.items[0]
			}
		}
	}
	return nil
}

func (tree *Tree) LastLeaf() (leaf *Leaf) {
	if tree.Root != nil {
		item := tree.Root
		for {
			if item.IsLeaf() {
				return item.(*Leaf)
			}

			if i, ok := item.(*Branch); ok {
				item = i.items[len(i.items)-1]
			}
		}
	}
	return nil
}
