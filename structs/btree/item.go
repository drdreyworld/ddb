package btree

type tItem struct {
	left   *tItem
	right  *tItem
	parent *tItem
	data   *Data
	tree   *BTree
}

type Data struct {
	key   int
	value interface{}
}

func NewData(key int, value interface{}) Data {
	return Data{key: key, value: value}
}

func (i *tItem) GetKey() int {
	return i.data.key
}

func (i *tItem) GetValue() interface{} {
	return i.data.value
}

func (i *tItem) SetValue(value interface{}) {
	i.data.value = value
}

func (i *tItem) IsRoot() bool {
	return i.parent == nil
}

func (i *tItem) isLeft() bool {
	return !i.IsRoot() && i.parent.left != nil && i.parent.left.GetKey() == i.GetKey()
}

func (i *tItem) isRight() bool {
	return !i.IsRoot() && i.parent.right != nil && i.parent.right.GetKey() == i.GetKey()
}

func (i *tItem) Left() *tItem {
	return i.left
}

func (i *tItem) Right() *tItem {
	return i.right
}

func (i *tItem) Parent() *tItem {
	return i.parent
}

func (i *tItem) Min() *tItem {
	if i.left != nil {
		return i.left.Min()
	}
	return i
}

func (i *tItem) Max() *tItem {
	if i.right != nil {
		return i.right.Max()
	}
	return i
}

func (i *tItem) Count() int {
	r := 1

	if i.left != nil {
		r += i.left.Count()
	}

	if i.right != nil {
		r += i.right.Count()
	}

	return r
}

func (i *tItem) Dispose() {
	i.parent = nil
	i.left = nil
	i.right = nil
	i.data = nil
	i = nil
}
