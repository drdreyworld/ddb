package qselect

import (
	"ddb/cdriver"
	"errors"
	"ddb/structs/types"
	"fmt"
)

type Select struct {
	Columns types.Columns
	From    From
	Where   types.Where
	Order   Order
	Limit   Limit
}

func CreateSelectFromString(q string) *Select {
	var b bool

	if _, q, b = matchAndReplace(reselect, q); !b {
		return nil
	}

	result := &Select{}

	q, result.Columns, b = ParseColumns(q)
	q, result.From, b = ParseFrom(q)
	q, result.Where, b = ParseWhere(q)
	q, result.Order, b = ParseOrder(q)
	q, result.Limit, b = ParseLimit(q)

	return result
}

var tables map[string]*cdriver.Table = map[string]*cdriver.Table{}

func getTable(name string) (tab *cdriver.Table, err error) {
	var ok bool
	if tab, ok = tables[name]; !ok {
		tables[name], err = cdriver.OpenTable(name)
		if err != nil {
			return nil, err
		}
		return tables[name], err
	}

	return tab, nil
}

func (s *Select) Execute() (rows *types.Rowset, err error) {
	var table *cdriver.Table
	var res *cdriver.DbResult

	if len(s.From) < 1 {
		return nil, errors.New("FROM statement is empty")
	}

	table, err = getTable(s.From[0].Value)

	if err != nil {
		return nil, err
	}

	res, err = table.Select(
		s.Columns,
		s.Where,
		s.Limit.RowCount,
		s.Limit.Offset,
	);

	if err != nil {
		return nil, err
	}

	rows = &types.Rowset{}

	for _, col := range s.Columns {
		rows.Cols = append(rows.Cols, types.Col{
			Name:   col.Value,
			Length: 255,
			Type:   15, //fieldTypeVarChar,
		})
	}

	if res != nil {
		for {
			if row, err := res.FetchMap(); err != nil {
				if err.Error() != "EOF" {
					fmt.Println("Fetch row Error:", err)
				}
				break
			} else {
				cells := []string{}

				for _, col := range s.Columns {
					cells = append(cells, row[col.Value])
				}

				rows.Rows = append(rows.Rows, cells)
			}
		}
	}

	return rows, nil
}