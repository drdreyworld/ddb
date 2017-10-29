package bptree

const SCAN_DIRECTION_ASC = 0
const SCAN_DIRECTION_DESC = 1

func (tree *Tree) ScanLeafs(fn func(row *Leaf), direction int) {
	var leaf *Leaf

	if direction == SCAN_DIRECTION_ASC {
		for leaf = tree.FirstLeaf(); leaf != nil; leaf = leaf.next {
			fn(leaf)
		}
	} else {
		for leaf = tree.LastLeaf(); leaf != nil; leaf = leaf.prev {
			fn(leaf)
		}
	}
}

func (tree *Tree) ScanRows(fn func(row *Row), direction int) {
	tree.ScanLeafs(func(leaf *Leaf) {
		if direction == SCAN_DIRECTION_ASC {
			leaf.ScanRowsASC(fn)
		} else {
			leaf.ScanRowsDESC(fn)
		}
	}, direction)
}
