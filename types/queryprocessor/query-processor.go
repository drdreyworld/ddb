package queryprocessor

import (
	"ddb/types/query"
	"errors"
	"fmt"
	"ddb/types/rowset"
	"ddb/types/table"
)

type QueryProcessor struct {
	tables map[string]*table.Table
}

func (qp *QueryProcessor) getTable(name string) (tab *table.Table, err error) {
	if qp.tables == nil {
		qp.tables = map[string]*table.Table{}
	}

	if tab, ok := qp.tables[name]; !ok {
		qp.tables[name], err = table.OpenTable(name)
		if err != nil {
			return nil, err
		}
		return qp.tables[name], err
	} else {
		return tab, nil
	}
}

func (qp *QueryProcessor) Execute(q interface{}) (*rowset.Rowset, error) {
	switch v := q.(type) {
	case *query.Select:
		return qp.executeSelect(v)
	default:
		return nil, errors.New("Unknown query type")
	}
}

func (qp *QueryProcessor) executeSelect(sel *query.Select) (*rowset.Rowset, error) {

	rows := &rowset.Rowset{}

	if len(sel.From) < 1 {
		return nil, errors.New("FROM statement is empty")
	}

	table, err := qp.getTable(sel.From[0].Value)

	if err != nil {
		return nil, err
	}

	res, err := table.Select(
		sel.Columns,
		sel.Where,
		sel.Order,
		sel.Limit,
	);

	if err != nil {
		return nil, err
	}

	for _, col := range sel.Columns {
		rows.Cols = append(rows.Cols, rowset.Col{
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

				for _, col := range sel.Columns {
					cells = append(cells, row[col.Value])
				}

				rows.Rows = append(rows.Rows, cells)
			}
		}
	}

	return rows, nil
}