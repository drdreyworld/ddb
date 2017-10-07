package btree

import (
	"ddb/types/key"
	"fmt"
)

type Items []*Item

type Item struct {
	Leaf   bool
	Items  Items
	Values Values
	Count  int
	Index  int
}

func (item *Item) Find(k key.BytesKey) *Value {
	if item == nil {
		return nil
	}

	i := 0
	j := item.Count

	for i < len(k) && j > 0 {
		if k[i] == item.Values[j-1].Key[i] {
			i++
		} else if k[i] < item.Values[j-1].Key[i] {
			j--
		} else {
			break
		}
	}

	if i == len(k) {
		return item.Values[j-1]
	}

	return item.Items[j].Find(k)
}

func (item *Item) SplitChild(tree *BTree, n int) {
	y := item.Items[n]

	z := tree.newItem()
	z.Leaf = y.Leaf

	z.Count = tree.Degree - 1
	y.Count = tree.Degree - 1

	for i := 0; i < tree.Degree; i++ {
		z.Values[i], y.Values[tree.Degree+i] = y.Values[tree.Degree+i], nil
		z.Items[i], y.Items[tree.Degree+i] = y.Items[tree.Degree+i], nil
	}

	for i := item.Count - 1; i > n; i-- {
		item.Values[i] = item.Values[i-1]
		item.Items[i+1] = item.Items[i]
	}

	item.Values[n] = y.Values[tree.Degree-1]
	y.Values[tree.Degree-1] = nil

	item.Items[n+1] = z
	item.Count++
}

func (item *Item) Insert(tree *BTree, value *Value) {
	if item.Leaf {
		i := 0
		j := item.Count

		for i < len(value.Key) && j > 0 {
			if item.Values[j-1].Key[i] == value.Key[i] {
				i++
			} else if item.Values[j-1].Key[i] > value.Key[i] {
				item.Values[j] = item.Values[j-1]
				j--
			} else {
				break
			}
		}

		if i == len(value.Key) {
			return
		}

		item.Values[j] = value
		item.Count++
	} else {
		pos := item.Count
		i := 0

		for pos > 0 {
			if value.Key[i] == item.Values[pos-1].Key[i] {
				i++
				if i == len(value.Key) {
					return
				}
				continue
			}

			if value.Key[i] > item.Values[pos-1].Key[i] {
				break
			}

			pos--
		}


		if item.Items[pos].Count == 2*tree.Degree-1 {
			item.SplitChild(tree, pos)

			cmpr := value.Key.Compare(item.Values[pos].Key)

			if cmpr == key.CMP_KEY_EQUAL {
				return
			}

			if cmpr == key.CMP_KEY_GREATHER {
				pos += 1
			}
		}

		item.Items[pos].Insert(tree, value)
	}
}


func (item *Item) InsertIfNotExists(tree *BTree, value *Value) *Value {
	if item.Leaf {
		i := 0
		j := item.Count

		for i < len(value.Key) && j > 0 {
			if item.Values[j-1].Key[i] == value.Key[i] {
				i++
			} else if item.Values[j-1].Key[i] > value.Key[i] {
				item.Values[j] = item.Values[j-1]
				j--
			} else {
				break
			}
		}

		if i == len(value.Key) {
			return item.Values[j-1]
		}

		item.Values[j] = value
		item.Count++

		return item.Values[j]
	} else {
		pos := item.Count
		i := 0

		for pos > 0 {
			aval := item.Values[pos-1]
			if aval == nil {
				pos--
				continue
			}

			if value.Key[i] == aval.Key[i] {
				i++
				if i == len(value.Key) {
					return item.Values[pos-1]
				}
				continue
			}

			if value.Key[i] > aval.Key[i] {
				break
			}

			pos--
		}

		if item.Items[pos].Count == 2*tree.Degree-1 {
			item.SplitChild(tree, pos)

			cmpr := value.Key.Compare(item.Values[pos].Key)

			if cmpr == key.CMP_KEY_EQUAL {
				return item.Values[pos]
			}

			if cmpr == key.CMP_KEY_GREATHER {
				pos += 1
			}
		}

		return item.Items[pos].InsertIfNotExists(tree, value)
	}
}

var d int

func (item *Item) DebugTree() {
	d++

	length := item.Count

	for j := 0; j < d-1; j++ {
		fmt.Print("|  ")
	}

	fmt.Print("i[", item.Count, "]\n")

	for i := 0; i <= length; i++ {
		if item.Items[i] != nil {
			item.Items[i].DebugTree()
		}

		if i < length && item.Values[i] != nil {
			for j := 0; j < d; j++ {
				fmt.Print("|  ")
			}

			fmt.Print(item.Values[i].Data, "\n")
		}
	}
	d--
}

func (item *Item) InfixTraverseItems(fn func(i *Item) bool) bool {
	if item == nil {
		return true
	}
	for i := 0; i <= item.Count; i++ {
		if item.Items[i] != nil && !item.Items[i].InfixTraverseItems(fn) {
			return false
		}
	}
	return fn(item)
}


func (item *Item) InfixTraverse(fn func(v *Value) bool) bool {
	for i := 0; i <= item.Count; i++ {
		if item.Items[i] != nil && !item.Items[i].InfixTraverse(fn) {
			return false
		}

		if item.Values[i] != nil && !fn(item.Values[i]) {
			return false
		}
	}
	return true
}

func (item *Item) PostfixTraverse(fn func(v *Value) bool) bool {
	if item == nil {
		return true
	}

	for i := item.Count; i >= 0; i-- {
		if item.Values[i] != nil && !fn(item.Values[i]) {
			return false
		}

		if item.Items[i] != nil && !item.Items[i].PostfixTraverse(fn) {
			return false
		}

	}
	return true
}
