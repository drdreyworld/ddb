package btree

import (
	"fmt"
	"ddb/types/key"
)

type Items []*Item

type Item struct {
	Leaf   bool
	Items  Items
	Values Values
	Count  int
	Index int
}

func (item *Item) Find(k key.BytesKey) *Value {
	if item == nil {
		return nil
	}

	pos := item.Count

	for ; pos > 0; pos-- {
		cmpr := item.Values[pos-1].Key.Compare(k)

		if cmpr == key.CMP_KEY_EQUAL {
			return item.Values[pos-1]
		} else if cmpr == key.CMP_KEY_LESS {
			break
		}
	}

	if pos < 0 {
		return nil
	}

	return item.Items[pos].Find(k)
}

func (item *Item) SplitChild(tree *BTree, n int) {
	y := item.Items[n]

	z := tree.newItem()
	z.Leaf = y.Leaf

	z.Count = tree.Degree-1
	y.Count = tree.Degree-1

	for i := 0; i < tree.Degree;i++ {
		z.Values[i], y.Values[tree.Degree+i] = y.Values[tree.Degree+i], nil
		z.Items[i], y.Items[tree.Degree+i] = y.Items[tree.Degree+i], nil
	}

	//copy(z.Values[:], y.Values[tree.Degree:])
	//
	//if !y.Leaf {
	//	copy(z.Items[:], y.Items[tree.Degree:])
	//}

	for i := item.Count - 1; i > n; i-- {
		item.Values[i] = item.Values[i-1]
		item.Items[i+1] = item.Items[i]
	}
	//copy(item.Values[n+1:], item.Values[n:])
	item.Values[n] = y.Values[tree.Degree-1]
	y.Values[tree.Degree-1] = nil

	//for i := n+1; i < item.Count; i++ {
	//	item.Items[i+1] = item.Items[i]
	//}
	//copy(item.Items[n+2:], item.Items[n+1:])
	item.Items[n+1] = z
	item.Count++
}

func (item *Item) Insert(tree *BTree, value *Value) {
	if item.Leaf {

		pos := item.Count
		for ; pos > 0; pos-- {
			cmpr := item.Values[pos-1].Key.Compare(value.Key)

			if cmpr == key.CMP_KEY_EQUAL {
				item.Values[pos-1] = value
				return
			}

			if cmpr == key.CMP_KEY_LESS {
				break
			}

			item.Values[pos] = item.Values[pos-1]
		}

		item.Values[pos] = value
		item.Count++
	} else {

		pos := item.Count

		for ; pos > 0; pos-- {
			cmpr := value.Key.Compare(item.Values[pos-1].Key)

			if cmpr == key.CMP_KEY_EQUAL {
				return
			}

			if cmpr == key.CMP_KEY_GREATHER {
				break
			}
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
