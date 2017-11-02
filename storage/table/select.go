package table

import (
	"ddb/storage/config"
	"ddb/storage/index"
	"ddb/types"
	"ddb/types/dbresult"
	"ddb/types/query"
	"errors"
	"fmt"
	"github.com/drdreyworld/smconv"
	"time"
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

func (t *Table) GetColumnsForIndex(cond types.CompareConditions, order query.Order) config.ColumnsConfig {
	cc := t.storage.GetColumnsConfig()
	indexColumns := config.ColumnsConfig{}
	indexColumnsMap := cc.GetMap()
	indexColumnsAdded := map[string]bool{}

	for i := range order {
		column := order[i].Column
		if _, ok := indexColumnsMap[column]; ok {
			if _, ok := indexColumnsAdded[column]; !ok {
				indexColumns = append(indexColumns, indexColumnsMap[column])
				indexColumnsAdded[column] = true
			}
		} else {
			panic("Column " + column + " not found")
		}
	}

	for i := range cond {
		column := cond[i].Field
		if _, ok := indexColumnsMap[column]; ok {
			if _, ok := indexColumnsAdded[column]; !ok {
				indexColumns = append(indexColumns, indexColumnsMap[column])
				indexColumnsAdded[column] = true
			}
		} else {
			panic("Column " + column + " not found")
		}
	}

	return indexColumns
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

	var idx index.Index
	indexColumns := t.GetColumnsForIndex(cond, sel.Order)

	if len(indexColumns) == 0 {
		return t.SearchWithoutIndex(cond, sel.Limit)
	}

	if idx = t.GetIndex(indexColumns); idx == nil {
		now := time.Now()
		fmt.Print("Build index: ")

		idx = t.CreateIndex(config.IndexConfig{Cols: indexColumns})
		idx.BuildIndex(t.storage)

		t.indexes = append(t.indexes, idx)
		fmt.Println(time.Now().Sub(now))
	}

	return t.SearchByIndex(idx, sel, cond)
}

func (t *Table) SearchByIndex(idx index.Index, sel *query.Select, cond types.CompareConditions) (res *dbresult.DbResult, err error) {

	conditions := cond.GroupByColumns()

	positions := []int{}

	st := time.Now()
	//fmt.Print("Search by index index: ", idx.GetName(), " ")
	fmt.Print("Search by index index: ")
	idx.ScanRows(
		func(column string, value []byte) bool {
			result := true
			for _, cnd := range conditions[column] {
				if result = result && cnd.Compare(value); !result {
					return result
				}
			}
			return result
		},
		func(pos []int) bool {
			for i := range pos {
				positions = append(positions, pos[i])
				if len(positions) >= sel.Limit.RowCount {
					return false
				}
			}
			return true
		},
		sel.Order.GetOrderMap(),
	)

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
				continue
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
