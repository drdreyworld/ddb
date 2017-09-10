package mbbtree

type BTree struct {
	root *TItem
}

func (t *BTree) CreateItem(data Data) *TItem {
	return &TItem{
		data:    &data,
		tree:    t,
		subtree: &BTree{},
	}
}

func (t *BTree) Add(data Data) bool {
	if t.root == nil {
		t.root = t.CreateItem(data)
		return true
	}
	p := t.root

	var cmp int
	for {
		cmp = p.GetKey().Compare(data.key)

		if cmp == CMP_KEY_EQUAL {
			p.data = &data
			return true
		}

		if cmp == CMP_KEY_GREATHER {
			if p.left == nil {
				p.left = t.CreateItem(data)
				p.left.parent = p
				return true
			}
			p = p.left
			continue
		}

		if cmp == CMP_KEY_LESS {
			if p.right == nil {
				p.right = t.CreateItem(data)
				p.right.parent = p
				return true
			}
			p = p.right
			continue
		}
	}
	return false
}

func (t *BTree) Find(key Key) *TItem {
	if t.root == nil {
		return nil
	}

	var cmp int

	for p := t.root; p != nil; {

		cmp = p.GetKey().Compare(key)

		if cmp == CMP_KEY_EQUAL {
			return p
		}

		if cmp == CMP_KEY_GREATHER {
			p = p.left
		} else {
			p = p.right
		}
	}

	return nil
}

func (t *BTree) FindLess(key Key) []*TItem {
	if t.root == nil {
		return nil
	}

	result := []*TItem{}

	t.root.InfixTraverse(func(i *TItem) (r bool) {
		if r = i.GetKey().Less(key); r {
			result = append(result, i)
		}
		return true
	})

	return result
}

func (t *BTree) FindGreather(key Key) []*TItem {
	if t.root == nil {
		return nil
	}

	result := []*TItem{}

	t.root.InfixTraverse(func(i *TItem) (r bool) {
		if r = i.GetKey().Greather(key); r {
			result = append(result, i)
		}
		return true
	})

	return result
}

func (t *BTree) Delete(key Key) bool {
	p := t.Find(key)

	if p == nil {
		return false
	}

	if p.left == nil && p.right == nil {
		if p.IsRoot() {
			t.root = nil
		} else if p.isLeft() {
			p.parent.left = nil
		} else if p.isRight() {
			p.parent.right = nil
		}
		return true
	}

	if p.left == nil {
		if p.IsRoot() {
			t.root = p.right
			t.root.parent = nil
		} else if p.isLeft() {
			p.parent.left = p.right
			p.parent.left.parent = p.parent
			p.Dispose()
		} else {
			p.parent.right = p.right
			p.parent.right.parent = p.parent
			p.Dispose()
		}
		return true
	}

	if p.right == nil {
		if p.IsRoot() {
			t.root = p.left
			t.root.parent = nil
		} else if p.isLeft() {
			p.parent.left = p.left
			p.parent.left.parent = p.parent
			p.Dispose()
		} else {
			p.parent.right = p.left
			p.parent.right.parent = p.parent
			p.Dispose()
		}
		p.Dispose()
		return true
	}

	r := p.right.Min()

	if r.parent.GetKey().Equal(p.GetKey()) {
		r.left = p.left

		if r.left != nil {
			r.left.parent = r
		}

		r.parent = p.parent

		if p.isLeft() {
			p.parent.left = r
		} else if p.isRight() {
			p.parent.right = r
		} else {
			t.root = r
		}
	} else {
		if r.right != nil {
			r.right.parent = r.parent
		}

		r.parent.left = r.right

		r.left = p.left
		r.left.parent = r

		r.right = p.right
		r.right.parent = r

		r.parent = p.parent

		if p.isLeft() {
			p.parent.left = r
		} else if p.isRight() {
			p.parent.right = r
		} else {
			t.root = r
		}
	}

	return true

}

func (t *BTree) Count() int {
	if t.root == nil {
		return 0
	}

	return t.root.Count()
}

func (t *BTree) Root() *TItem {
	return t.root
}
