package storage

import (
	"ddb/types/query"
	"ddb/types/rowset"
	"errors"
	"fmt"
)

type QueryProcessor struct {
	tm *TableManager
}

func (qp *QueryProcessor) Init(tm *TableManager) {
	qp.tm = tm
}

func (qp *QueryProcessor) Execute(q interface{}, qs string) (*rowset.Rowset, int, error) {
	//log, err := qp.tm.getTable("log")
	//if err != nil {
	//	panic(err)
	//}
	//log.Insert(&query.Insert{
	//	Table: "log",
	//	Columns: query.Columns{"time", "query"},
	//	Values: query.Values{
	//		query.ValuesRow{
	//			query.Value{
	//				Data: time.Now().Format("2006-01-02 15:04:05"),
	//			},
	//			query.Value{
	//				Data: qs,
	//			},
	//		},
	//	},
	//})
	//
	switch v := q.(type) {
	case *query.Select:
		return qp.executeSelect(v)
	case *query.Insert:
		return qp.executeInsert(v)
	default:
		return nil, 0, errors.New("Unknown query type")
	}
}

func (qp *QueryProcessor) executeSelect(sel *query.Select) (*rowset.Rowset, int, error) {

	rows := &rowset.Rowset{}

	if len(sel.From) < 1 {
		return nil, 0, errors.New("FROM statement is empty")
	}

	tab, err := qp.tm.getTable(sel.From[0].Value)

	if err != nil {
		return nil, 0, err
	}

	res, err := tab.Select(sel)

	if err != nil {
		return nil, 0, err
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

	return rows, 0, nil
}

func (qp *QueryProcessor) executeInsert(ins *query.Insert) (*rowset.Rowset, int, error) {
	tab, err := qp.tm.getTable(string(ins.Table))

	if err != nil {
		return nil, 0, err
	}

	if err := tab.Insert(ins); err != nil {
		return nil, 0, err
	}

	return nil, len(ins.Values), nil
}
