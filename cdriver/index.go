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
			tree.Add(mbtree.NewData(int(key), []int{pos}))
			item = tree.Find(key)
			tree = item.GetSubTree()
		} else {
			if ps, ok := item.GetValue().([]int); ok {
				if !posInSlice(pos, ps) {
					ps = append(ps, pos)
					item.SetValue(ps)
				}
			}
			tree = item.GetSubTree()
		}
	}

	if ps, ok := item.GetValue().([]int); ok {
		if !posInSlice(pos, ps) {
			ps = append(ps, pos)
			item.SetValue(ps)
		}
	} else {
		item.SetValue([]int{pos})
	}
}

func posInSlice(pos int, ps []int) bool {
	for _, v := range ps {
		if v == pos {
			return true
		}
	}
	return false
}

func (i *Index) Find(row map[string][]byte, limit, offset int) (res *DbResult) {
	var item *mbtree.TItem
	var tree *mbtree.BTree

	res = &DbResult{}
	res.Init(i.table)

	for _, column := range i.Columns {
		if val, ok := row[column]; ok {
			key := i.GetCRC(val)

			if item == nil {
				tree = &i.tree
			} else {
				tree = item.GetSubTree()
			}

			if item = tree.Find(int(key)); item == nil {
				return nil
			}
		} else {
			break
		}
	}

	if item == nil {
		return nil
	}

	if ps, ok := item.GetValue().([]int); ok {
		if offset < len(ps) {
			if limit > len(ps)-offset {
				limit = len(ps) - offset
			}
			ps = ps[offset : offset+limit]
			res.SetPositions(ps)
		} else {
			res.SetPositions([]int{})
		}
	} else {
		panic("Can't convert index data to []int")
	}
	return res
}
