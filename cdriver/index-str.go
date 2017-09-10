package cdriver

import (
	"ddb/structs/mbbtree"
)

type IndexStr struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	tree    mbbtree.BTree
	table   *Table
}

func (i *IndexStr) Init(t *Table) {
	i.table = t
}

func (i *IndexStr) Add(pos int, row map[string][]byte) {
	var item *mbbtree.TItem

	tree := &i.tree

	for _, column := range i.Columns {
		key := row[column]

		if item = tree.Find(key); item == nil {
			tree.Add(mbbtree.NewData(key, []int{pos}))
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

func (i *IndexStr) Find(cond []FindFieldCond, limit, offset int) (res *DbResult) {

	var items []*mbbtree.TItem
	var trees []*mbbtree.BTree

	res = &DbResult{}
	res.Init(i.table)

	trees = []*mbbtree.BTree{}

	for j, column := range i.Columns {
		if j >= len(cond) {
			break
		}
		if cond[j].Field != column {
			panic("Column mistmatch " + cond[j].Field + " vs " + column)
		}

		if len(items) == 0 {
			trees = []*mbbtree.BTree{&i.tree}
		} else {
			trees = []*mbbtree.BTree{}
			for _, item := range items {
				trees = append(trees, item.GetSubTree())
			}
		}

		cnd := cond[j]

		switch cnd.Compartion {
		case "=":
			items = []*mbbtree.TItem{}
			for _, tree := range trees {
				if item := tree.Find(cnd.Value); item != nil {
					items = append(items, item)
				}
			}
			if len(items) == 0 {
				return nil
			}
			break
		case "<":
			items = []*mbbtree.TItem{}
			for _, tree := range trees {
				items = append(items, tree.FindLess(cnd.Value)...)
			}
			if len(items) == 0 {
				return nil
			}
			break
		case ">":
			items = []*mbbtree.TItem{}
			for _, tree := range trees {
				items = append(items, tree.FindGreather(cnd.Value)...)
			}
			if len(items) == 0 {
				return nil
			}
			break
		}
	}

	if len(items) == 0 {
		return nil
	}

	psmap := map[int]int{}

	for _, item := range items {
		if poss, ok := item.GetValue().([]int); ok {
			for _, p := range poss {
				psmap[p] = p
			}
		}
	}

	ps := []int{}

	for p := range psmap {
		ps = append(ps, p)
	}

	psmap = nil

	if limit == 0 && offset == 0 {
		limit = len(ps)
		offset = 0
	}

	if offset < len(ps) {
		if limit > len(ps)-offset {
			limit = len(ps) - offset
		}
		ps = ps[offset : offset+limit]
		res.SetPositions(ps)
	} else {
		res.SetPositions([]int{})
	}

	return res
}
