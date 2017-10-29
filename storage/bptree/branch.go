package bptree

type Branch struct {
	tree  *Tree
	keys  Keys
	items Items
}

func (item *Branch) IsLeaf() bool {
	return false
}

func (item *Branch) IsFull() bool {
	return len(item.keys) > 2*item.tree.Mid-1
}

func (item *Branch) Find(key Key) *Row {
	i := 0
	for ; i < len(item.keys); i++ {
		if key.Less(item.keys[i]) {
			break
		}
	}

	return item.items[i].Find(key)
}

func (item *Branch) Insert(row *Row) {
	i := 0
	key := row.Key()
	for ; i < len(item.keys); i++ {
		if key.Less(item.keys[i]) {
			break
		}
	}
	item.items[i].Insert(row)
	item.items[i].split(item)
}

func (item *Branch) split(branch *Branch) {
	n := item.tree.Mid
	if len(item.keys) > 2*n-1 {

		key := item.keys[n-1]

		bnew := item.tree.createBranch()
		bnew.keys = make(Keys, len(item.keys)-n)
		copy(bnew.keys, item.keys[n:])

		item.keys = item.keys[:n-1]

		bnew.items = make(Items, len(item.items)-n)
		copy(bnew.items, item.items[n:])

		item.items = item.items[:n]

		i := branch.keys.Insert(key)

		branch.items = append(branch.items, nil)
		copy(branch.items[i+1:], branch.items[i:])

		branch.items[i] = item
		branch.items[i+1] = bnew
	}
}