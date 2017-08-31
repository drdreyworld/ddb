package btree

type BTree struct {
	root *tItem
}

func (t *BTree) CreateItem(data Data) *tItem {
	return &tItem{
		data: &data,
		tree: t,
	}
}

func (t *BTree) Add(data Data) bool {
	if t.root == nil {
		t.root = t.CreateItem(data)
		return true
	}
	p := t.root

	for {
		if p.GetKey() == data.key {
			p.data = &data
			return true
		}

		if p.GetKey() > data.key {
			if p.left == nil {
				p.left = t.CreateItem(data)
				p.left.parent = p
				return true
			}
			p = p.left
			continue
		}
		if p.GetKey() < data.key {
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

func (t *BTree) Find(key int) *tItem {
	if t.root == nil {
		return nil
	}

	for p := t.root; p != nil; {
		if p.GetKey() == key {
			return p
		}

		if p.GetKey() > key {
			p = p.left
		} else {
			p = p.right
		}
	}

	return nil
}

func (t *BTree) Delete(key int) bool {
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

	if r.parent.GetKey() == p.GetKey() {
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

func (t *BTree) Root() *tItem {
	return t.root
}
