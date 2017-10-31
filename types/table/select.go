package table

import (
	"ddb/types"
	"ddb/types/index"
	"errors"
	"fmt"
	"time"
	"ddb/types/query"
	"ddb/types/dbresult"
	"ddb/types/key"
	"ddb/types/config"
	"github.com/drdreyworld/smconv"
)

func (t *Table) CreateFindCond(where query.Where) (res []types.CompareCondition, err error) {
	res = []types.CompareCondition{}
	for i := range where {
		c := t.config.Columns.ByName(where[i].OperandA)
		if c == nil {
			return nil, errors.New("Unknown column " + where[i].OperandA)
		}

		res = append(res, types.CompareCondition{
			Field:      where[i].OperandA,
			Value:      smconv.StringValueToBytes(where[i].OperandB, c.Type, c.Length),
			Compartion: where[i].Compartion,
		})
	}
	return res, nil
}

func (t *Table) GetIndex(indexColumns config.ColumnsConfig) index.Index {
	for _, idx := range t.indexes {
		if ok := len(indexColumns) == len(idx.GetColumns()); ok {

			for i, column := range idx.GetColumns() {
				if ok = indexColumns[i].Name == column.Name; !ok {
					break
				}
			}

			if ok {
				return idx
			}
		}
	}
	return nil
}

func (t *Table) Select(sel *query.Select) (res *dbresult.DbResult, err error) {

	sel.Columns.PrepareColumns(t.config.Columns)
	sel.Limit.PrepareLimit(t.storage.GetRowsCount())

	var cond types.CompareConditions

	if cond, err = t.CreateFindCond(sel.Where); err != nil {
		return nil, err
	}

	idx := CreateIndex(t.config.IndexType)
	indexColumns := idx.GetColumnsForIndex(t.storage.GetColumnsConfig(), cond, sel.Order)

	if len(indexColumns) == 0 {
		return t.SearchWithoutIndex(cond, sel.Limit)
	}

	if idx = t.GetIndex(indexColumns); idx == nil {
		now := time.Now()
		fmt.Print("Build index: ")

		idx = CreateIndex(t.config.IndexType)
		idx.Init()
		idx.SetTableName(t.name)
		idx.SetColumns(indexColumns)
		idx.BuildIndex(t.storage)
		idx.GenerateName("tmpidx")
		idx.SetTemporaryFlag(true)

		t.indexes = append(t.indexes, idx)
		fmt.Println(time.Now().Sub(now))
	}

	return t.SearchByIndex(idx, sel, cond)
}

func (t *Table) SearchByIndex(idx index.Index, sel *query.Select, cond types.CompareConditions) (res *dbresult.DbResult, err error) {

	ordersColumns := sel.Order.GetOrderMap()
	conds := cond.GroupByColumns()

	whereCallback := func(column string, value key.BytesKey) bool {
		result := true
		for _, cnd := range conds[column] {
			if result = result && cnd.Compare(value); !result {
				return result
			}

		}
		return result
	}

	positions := []int{}

	st := time.Now()
	fmt.Print("Search by index index: ", idx.GetName(), " ")
	idx.Traverse(ordersColumns, whereCallback, func(pos []int) bool {
		for i := range pos {
			positions = append(positions, pos[i])
			if len(positions) >= sel.Limit.RowCount {
				return false
			}
		}
		return true
	})
	fmt.Println(time.Now().Sub(st))

	res = &dbresult.DbResult{}
	res.Init(t.storage)
	res.SetPositions(positions)
	return res, nil
}

func (t *Table) SearchWithoutIndex(cond types.CompareConditions, limit query.Limit) (res *dbresult.DbResult, err error) {
	now := time.Now()
	fmt.Print("Search without index: ")

	conds := cond.GroupByColumns()

	positions := make([]int, 0, limit.RowCount)
	j := 0

	rowsCount := t.storage.GetRowsCount()

	for i := 0; i < rowsCount; i++ {
		matched := true

		for ci := range t.config.Columns {
			column := &t.config.Columns[ci]

			if _, ok := conds[column.Name]; !ok {
				continue;
			}

			b := t.storage.GetBytesByColumnIndex(i, ci)

			for _, columnCond := range conds[column.Name] {
				if matched = columnCond.Compare(b); !matched {
					break
				}
			}
			if !matched {
				break
			}
		}

		if matched {
			if j >= limit.Offset {
				positions = append(positions, i)
			}
			j++
			if j >= limit.RowCount {
				break
			}
		}
	}

	res = &dbresult.DbResult{}
	res.Init(t.storage)
	res.SetPositions(positions)

	fmt.Println(time.Now().Sub(now))
	return res, nil
}
