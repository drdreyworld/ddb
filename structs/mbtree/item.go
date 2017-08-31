package mbtree

type TItem struct {
	left    *TItem
	right   *TItem
	parent  *TItem
	data    *Data
	tree    *BTree
	subtree *BTree
}

type Data struct {
	key   int
	value interface{}
}

func NewData(key int, value interface{}) Data {
	return Data{key: key, value: value}
}

func (i *TItem) GetKey() int {
	return i.data.key
}

func (i *TItem) GetValue() interface{} {
	return i.data.value
}

func (i *TItem) GetSubTree() *BTree {
	return i.subtree
}

func (i *TItem) SetValue(value interface{}) {
	i.data.value = value
}

func (i *TItem) IsRoot() bool {
	return i.parent == nil
}

func (i *TItem) isLeft() bool {
	return !i.IsRoot() && i.parent.left != nil && i.parent.left.GetKey() == i.GetKey()
}

func (i *TItem) isRight() bool {
	return !i.IsRoot() && i.parent.right != nil && i.parent.right.GetKey() == i.GetKey()
}

func (i *TItem) Left() *TItem {
	return i.left
}

func (i *TItem) Right() *TItem {
	return i.right
}

func (i *TItem) Parent() *TItem {
	return i.parent
}

func (i *TItem) Min() *TItem {
	if i.left != nil {
		return i.left.Min()
	}
	return i
}

func (i *TItem) Max() *TItem {
	if i.right != nil {
		return i.right.Max()
	}
	return i
}

func (i *TItem) Count() int {
	r := 1

	if i.left != nil {
		r += i.left.Count()
	}

	if i.right != nil {
		r += i.right.Count()
	}

	return r
}

func (i *TItem) Dispose() {
	i.parent = nil
	i.left = nil
	i.right = nil
	i.data = nil
	i = nil
}
