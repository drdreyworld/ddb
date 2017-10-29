package bptree

import "ddb/types/funcs"

func (tree *Tree) SaveLeaf(leaf *Leaf) {
	if tree.file != nil && leaf.IsLoaded() {
		pagesize := tree.GetPageSize()
		rowsize := tree.GetRowSize()

		offset := int64(int(leaf.page) * pagesize)
		page := make([]byte, pagesize)

		pos := 0
		copy(page[0:], funcs.Int32ToBytes(leaf.page))

		pos += 5
		if leaf.next != nil {
			copy(page[pos:], funcs.Int32ToBytes(leaf.next.page))
		} else {
			copy(page[pos:], funcs.Int32ToBytes(int32(-1)))
		}

		pos += 5
		copy(page[pos:], funcs.Int32ToBytes(int32(leaf.count)))

		pos += 5
		for i := 0; i < leaf.count; i++ {
			copy(page[pos+i*rowsize:pos+(i+1)*rowsize], *leaf.rows.GetRow(i))
		}

		if _, err := tree.file.WriteAt(page, offset); err != nil {
			panic(err)
		}
	}
}

func (tree *Tree) LoadLeaf(leaf *Leaf) {
	if tree.file != nil && !leaf.IsLoaded() {

		bytes := make([]byte, tree.GetPageSize())
		offset := int64(int(leaf.page) * tree.GetPageSize())

		if _, err := tree.file.ReadAt(bytes, offset); err != nil {
			panic(err)
		}

		leaf.count = int(funcs.Int32FromBytes(bytes[10:15]))
		pos := tree.GetPageInfoSize()

		rowsize := tree.GetRowSize()
		for i := 0; i < leaf.count; i++ {
			row := Row(bytes[pos+i*rowsize : pos+(i+1)*rowsize])
			leaf.rows.SetRow(i, &row)
		}
	}
}