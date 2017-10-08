package btree

import "ddb/types/key"

type BTree struct {
	Root   *Item
	Degree int
	Count  int
}

func (tree *BTree) Find(key key.BytesKey) *Value {
	if tree.Root == nil {
		return nil
	}

	return tree.Root.Find(key)
}

func (tree *BTree) newItem() *Item {
	tree.Count++
	return &Item{
		Count:  0,
		Values: make(Values, 2*tree.Degree),
		Items:  make(Items, 2*tree.Degree),
		Index:  tree.Count,
	}
}

func (tree *BTree) Insert(value *Value) {
	if tree.Root == nil {
		tree.Root = tree.newItem()
		tree.Root.Leaf = true
		tree.Root.Values[0] = value
		tree.Root.Count = 1
		return
	}

	if tree.Root.Count == 2*tree.Degree-1 {
		newRoot := tree.newItem()
		newRoot.Leaf = false
		newRoot.Count = 0
		newRoot.Items[0] = tree.Root

		tree.Root = newRoot
		tree.Root.SplitChild(tree, 0)
	}
	tree.Root.Insert(tree, value)
}