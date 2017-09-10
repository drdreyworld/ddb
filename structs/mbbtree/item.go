package mbbtree

type TItem struct {
	left    *TItem
	right   *TItem
	parent  *TItem
	data    *Data
	tree    *BTree
	subtree *BTree
}

type Key []byte

const (
	CMP_KEY_EQUAL    = 0
	CMP_KEY_LESS     = -1
	CMP_KEY_GREATHER = 1
)

func (k Key) Equal(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_EQUAL
}

func (k Key) Less(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_LESS
}

func (k Key) Greather(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_GREATHER
}

func (k Key) Compare(key Key) (r int) {
	lk := len(k)
	lK := len(key)

	l := lk

	if lK > l {
		l = lK
	}

	var a, b byte

	for i := 0; i < l; i++ {
		if i >= lk {
			a = 0
		} else {
			a = (k)[i]
		}

		if i >= lK {
			b = 0
		} else {
			b = key[i]
		}

		if a < b {
			return CMP_KEY_LESS
		} else if a > b {
			return CMP_KEY_GREATHER
		}
	}

	return CMP_KEY_EQUAL
}

type Data struct {
	key   Key
	value interface{}
}

func NewData(key Key, value interface{}) Data {
	return Data{key: key, value: value}
}

func (i *TItem) GetKey() Key {
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
	return !i.IsRoot() && i.parent.left != nil && i.parent.left.GetKey().Equal(i.GetKey())
}

func (i *TItem) isRight() bool {
	return !i.IsRoot() && i.parent.right != nil && i.parent.right.GetKey().Equal(i.GetKey())
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

func (i *TItem) InfixTraverse(fn func(i *TItem) bool) bool {
	if i.left != nil {
		if !i.left.InfixTraverse(fn) {
			return false
		}
	}

	if !fn(i) {
		return false
	}

	if i.right != nil {
		if !i.right.InfixTraverse(fn) {
			return false
		}
	}

	return true
}
