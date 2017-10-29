package bptree

import (
	"sync"
)

type Leaf struct {
	sync.Mutex
	tree    *Tree
	rows    Rows
	page    int32
	next    *Leaf
	prev    *Leaf
	count   int
	loaded  bool
	changed bool
}

func (item *Leaf) IsLeaf() bool {
	return true
}

func (item *Leaf) Count() int {
	return item.count
}

func (item *Leaf) IsFull() bool {
	return item.count > 2*item.tree.Mid-1
}

func (item *Leaf) IsChanged() bool {
	return item.changed
}

func (item *Leaf) IsLoaded() bool {
	return item.loaded
}

func (item *Leaf) Find(key Key) *Row {
	item.Load()
	item.Lock()
	defer item.Unlock()

	for i := 0; i < item.count; i++ {
		row := item.rows.GetRow(i)
		if key.Equal(row.Key()) {
			return row
		}
	}
	return nil
}

func (item *Leaf) Insert(row *Row) {
	item.Load()
	item.Lock()
	defer item.Unlock()

	item.changed = true
	key := row.Key()

	i := item.count
	for i > 0 {
		irow := item.rows.GetRow(i - 1)
		cmpr := key.Compare(irow.Key())

		if cmpr == EQUAL {
			item.rows.SetRow(i-1, row)
			return
		}

		if cmpr == MORE {
			break
		}

		item.rows.SetRow(i, irow)
		i--
	}

	item.rows.SetRow(i, row)
	item.count++
}

func (item *Leaf) split(branch *Branch) {
	n := item.tree.Mid
	if item.IsFull() {
		item.Load()
		item.Lock()

		leaf := item.tree.createLeaf()
		leaf.changed = true
		leaf.count = item.count - n
		copy(leaf.rows, item.rows[n:])

		copy(item.rows, item.rows[:n])
		item.count = n

		leaf.prev = item
		leaf.next = item.next
		item.next = leaf

		i := branch.keys.Insert(leaf.rows[0].Key())

		branch.items = append(branch.items, nil)
		copy(branch.items[i+1:], branch.items[i:])

		branch.items[i] = item
		branch.items[i+1] = leaf

		item.Unlock()

		item.Save()
		leaf.Save()
	} else {
		item.Save()
	}
}

func (item *Leaf) Unload() {
	item.Lock()
	defer item.Unlock()

	if item.IsLoaded() {
		item.loaded = false
		item.rows = make(Rows, item.tree.Mid*2)
	}
}

func (item *Leaf) Load() {
	item.Lock()
	defer item.Unlock()

	if !item.IsLoaded() {
		item.tree.LoadLeaf(item)
		item.loaded = true
	}
}

func (item *Leaf) Save() {
	item.tree.savechan <- item
}

func (item *Leaf) ScanRowsASC(fn func(row *Row)) {
	item.Load()
	for i := 0; i < item.count; i++ {
		fn(item.rows.GetRow(i))
	}
}

func (item *Leaf) ScanRowsDESC(fn func(row *Row)) {
	item.Load()
	for i := item.count; i > 0; i-- {
		fn(item.rows.GetRow(i))
	}
}
