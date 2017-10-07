package table

import (
	"ddb/types/funcs"
	"ddb/types"
	"ddb/types/index"
	"errors"
	"fmt"
	"strconv"
	"time"
	"ddb/types/query"
	"ddb/types/dbresult"
	"ddb/types/key"
)

func (t *Table) CreateFindCond(where query.Where) (res []types.CompareCondition, err error) {
	res = []types.CompareCondition{}
	for i := range where {
		c := t.config.Columns.ByName(where[i].OperandA)
		if c == nil {
			return nil, errors.New("Unknown column " + where[i].OperandA)
		}

		switch c.Type {

		case "int32":
			val, err := strconv.Atoi(where[i].OperandB)
			if err != nil {
				return nil, err
			}

			ival, err := funcs.ValueToBytes(val, c.Length)
			if err != nil {
				return nil, err
			}

			res = append(res, types.CompareCondition{
				Field:      where[i].OperandA,
				Value:      ival,
				Compartion: where[i].Compartion,
			})
			break

		case "string":
			sval, err := funcs.ValueToBytes(where[i].OperandB, c.Length)
			if err != nil {
				return nil, err
			}

			res = append(res, types.CompareCondition{
				Field:      where[i].OperandA,
				Value:      sval,
				Compartion: where[i].Compartion,
			})

			break
		default:
			return nil, errors.New("Unknown column type " + c.Type)
		}
	}
	return res, nil
}

func (t *Table) GetIndex(cond types.CompareConditions, order query.Order) index.Index {
	columnsMap := map[string]bool{}

	for i := range cond {
		columnsMap[cond[i].Field] = true
	}

	for i := range order {
		columnsMap[order[i].Column] = true
	}

	for _, idx := range t.indexes {
		if ok := len(columnsMap) == len(idx.GetColumns()); ok {
			for _, column := range idx.GetColumns() {
				if _, ok = columnsMap[column.Name]; !ok {
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

	// select * magic >>>
	cols := query.SelectExprs{}
	selallcols := false
	for i := range sel.Columns {
		if sel.Columns[i].Value == "*" {
			if !selallcols {
				for _, col := range t.config.Columns {
					cols = append(cols, query.SelectExpr{
						Value: col.Name,
						Type: query.SEL_EXPR_TYPE_COLUMN,
					})
				}
				selallcols = true
			} else {
				continue;
			}
		} else {
			cols = append(cols, sel.Columns[i])
		}
	}

	sel.Columns = cols
	// <<< select * magic

	sel.Limit.PrepareLimit(t.storage.GetRowsCount())

	var cond types.CompareConditions
	var whereCallback index.WhereCallback

	if cond, err = t.CreateFindCond(sel.Where); err != nil {
		return nil, err
	}

	ordersColumns := map[string]string{}

	for _, order := range sel.Order {
		ordersColumns[order.Column] = order.Direction
	}

	var idx index.Index

	if idx = t.GetIndex(cond, sel.Order); idx == nil {
		if len(ordersColumns) == 0 {
			return t.SearchWithoutIndex(sel.Columns, cond, sel.Limit)
		} else {
			now := time.Now()
			fmt.Print("Build index: ")
			idx = CreateIndex(t.config.IndexType)
			idx.Init("tmpidx", t.name)

			if !idx.BuildIndex(t.storage, cond, sel.Order) {
				fmt.Println("index not need: ", time.Now().Sub(now))
				return t.SearchWithoutIndex(sel.Columns, cond, sel.Limit)
			}

			fmt.Println(time.Now().Sub(now))
			whereCallback = nil
		}
	} else {
		fmt.Println("Use index:", idx.GetName())
		conds := cond.GroupByColumns()

		whereCallback = func(column string, value key.BytesKey) bool {
			result := true
			for _, cnd := range conds[column] {
				if result = result && cnd.Compare(value); !result {
					return result
				}
			}
			return result
		}
	}

	for _, column := range idx.GetColumns() {
		if _, ok := ordersColumns[column.Name]; !ok {
			ordersColumns[column.Name] = "ASC"
		}
	}

	positions := []int{}

	st := time.Now()
	fmt.Print("Traverse index: ")
	idx.Traverse(ordersColumns, whereCallback, func(pos []int) bool {
	//idx.Traverse(ordersColumns, nil, func(pos []int) bool {
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

func (t *Table) SearchWithoutIndex(cols query.SelectExprs, cond types.CompareConditions, limit query.Limit) (res *dbresult.DbResult, err error) {
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
