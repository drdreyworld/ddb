package bptree

import (
	"ddb/types/funcs"
)

func (tree *Tree) LoadTree() error {
	if tree.file != nil {
		pagesize := tree.GetPageSize()
		infosize := tree.GetPageInfoSize() + 4 + tree.keySize

		s, err := tree.file.Stat()
		if err != nil {
			return err
		}

		pagescount := int(s.Size() / int64(pagesize))

		leafs := make([]*Leaf, pagescount)
		keys := make([]Key, pagescount)
		nexts := make([]int, pagescount)
		pages := make([]int, pagescount)

		for i := 0; i < pagescount; i++ {

			offset := int64(i * pagesize)
			bytes := make([]byte, infosize)

			if _, err := tree.file.ReadAt(bytes, offset); err != nil {
				return err
			}

			leafs[i] = &Leaf{
				tree:  tree,
				count: int(funcs.Int32FromBytes(bytes[10:15])),
				page:  int32(i),
				rows:  make(Rows, tree.Mid*2),
			}

			keys[i] = bytes[15+4 : 15+4+funcs.Uint32FromBytes(bytes[15:15+4])]

			nexts[i] = int(funcs.Int32FromBytes(bytes[5:10]))
		}

		tree.pagesCount = int32(pagescount)

		if len(leafs) == 0 {
			return nil
		}

		if len(leafs) == 1 {
			tree.Root = leafs[0]
			return nil
		}

		for i := 0; i < len(leafs); i++ {
			if nexts[i] > -1 {
				leafs[i].next = leafs[nexts[i]]
				leafs[nexts[i]].prev = leafs[i]
			}
		}

		first := leafs[0]

		for first.prev != nil {
			first = first.prev
		}

		for i := 0; first != nil; first = first.next {
			pages[i] = int(first.page)
			i++
		}

		tree.Root = tree.reconstructTree(pages, keys, leafs)
	}

	return nil
}

func (tree *Tree) reconstructTree(pages []int, keys []Key, leafs []*Leaf) Item {
	var reconstructTreeFunc func(min, max int) Item

	itemsInBranch := (tree.Mid * 2) - 1

	reconstructTreeFunc = func(min, max int) Item {

		pagesLength := (max - min)
		chunkSize := funcs.DivRoundUp(pagesLength, itemsInBranch)
		chunksCount := funcs.DivRoundUp(pagesLength, chunkSize)

		branch := tree.createBranch()
		branch.keys = make(Keys, chunksCount-1)
		branch.items = make(Items, chunksCount)

		for i := 1; i < chunksCount; i++ {
			page := pages[min+(i*chunkSize)]
			branch.keys[i-1] = keys[page]
		}

		if chunkSize == 1 {
			for i := 0; i < chunksCount; i++ {
				page := pages[min+(i*1)]
				branch.items[i] = leafs[page]
			}
		} else {
			for i := 0; i < chunksCount; i++ {
				m := min + ((i + 0) * chunkSize)
				n := min + ((i + 1) * chunkSize)

				if n > max {
					n = max
				}

				branch.items[i] = reconstructTreeFunc(m, n)
			}
		}

		return branch
	}

	return reconstructTreeFunc(0, len(pages))
}
