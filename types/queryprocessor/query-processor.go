package queryprocessor

import (
	"ddb/types/query"
	"errors"
	"fmt"
	"ddb/types/rowset"
	"ddb/types/table"
	"sync"
	"time"
)

type QueryProcessor struct {
	tables map[string]*table.Table
	states map[string]string
}

func (qp *QueryProcessor) Init() {
	qp.tables = map[string]*table.Table{}
	qp.states = map[string]string{}
}

func (qp *QueryProcessor) CloseTables() {
	for i := range qp.tables {
		qp.tables[i].Close()
	}
}

func (qp *QueryProcessor) GetState(name string) (state string) {
	var ok bool
	if state, ok = qp.states[name]; !ok {
		qp.states[name] = "undefined"
	}
	state = qp.states[name]
	return state
}

func (qp *QueryProcessor) SetState(name, state string) {
	qp.states[name] = state
}

func (qp *QueryProcessor) getTable(name string) (tab *table.Table, err error) {
	var mutex = &sync.Mutex{}

	mutex.Lock()
	state := qp.GetState(name)
	canOpen := state == "undefined"
	if canOpen {
		state = "opening"
		qp.SetState(name, state)
	}
	mutex.Unlock()

	if !canOpen {
		for state != "opened" {
			mutex.Lock()
			state = qp.states[name]
			mutex.Unlock()

			if state == "opened" {
				break
			}
			fmt.Println("Wait for table opening")
			time.Sleep(500 * time.Millisecond)
		}
	}

	if _, ok := qp.tables[name]; !ok {
		qp.tables[name], err = table.OpenTable(name)
		if err != nil {
			return nil, err
		}

		qp.states[name] = "opened"

		return qp.tables[name], err
	}

	return qp.tables[name], nil
}

func (qp *QueryProcessor) Execute(q interface{}) (*rowset.Rowset, int, error) {
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

	tab, err := qp.getTable(sel.From[0].Value)

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
	tab, err := qp.getTable(string(ins.Table))
	if err != nil {
		return nil, 0, err
	}

	if err := tab.Insert(ins); err != nil {
		return nil, 0, err
	}

	tab.Save()

	return nil, len(ins.Values), nil
}