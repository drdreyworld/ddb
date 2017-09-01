package cdriver

import (
	"ddb/structs/mbtree"
	"hash/crc32"
)

type Index struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	tree    mbtree.BTree
	crc32q  *crc32.Table
	table   *Table
}

type treeItem struct {
	Value     uint32
	Positions []int
}

func (i *Index) Init(t *Table) {
	i.table = t
	i.crc32q = crc32.MakeTable(0xD5828281)
}

func (i *Index) GetCRC(data []byte) int {
	return int(crc32.Checksum(data, i.crc32q))
}

func (i *Index) Add(pos int, row map[string][]byte) {
	var item *mbtree.TItem

	tree := &i.tree

	for _, column := range i.Columns {
		key := i.GetCRC(row[column])

		if item = tree.Find(key); item == nil {
			// tree.Add(mbtree.NewData(int(key), map[int]int{pos:pos}))
			tree.Add(mbtree.NewData(int(key), nil))
			item = tree.Find(key)
			tree = item.GetSubTree()
		} else {
			tree = item.GetSubTree()
		}
	}

	if ps, ok := item.GetValue().(map[int]int); ok {
		ps[pos] = pos
		item.SetValue(ps)
	} else {
		item.SetValue(map[int]int{pos: pos})
	}
}

func (i *Index) Find(row map[string][]byte) (res *DbResult) {
	var item *mbtree.TItem
	var tree *mbtree.BTree

	res = &DbResult{}
	res.Init(i.table)

	for _, column := range i.Columns {
		key := i.GetCRC(row[column])

		if item == nil {
			tree = &i.tree
		} else {
			tree = item.GetSubTree()
		}

		if item = tree.Find(int(key)); item == nil {
			return nil
		}
	}

	if ps, ok := item.GetValue().(map[int]int); ok {
		res.SetPositions(ps)
	} else {
		panic("Can't convert data to map[int]int")
	}
	return res
}
