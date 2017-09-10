package qselect

import (
	"ddb/cdriver"
	"errors"
)

type Select struct {
	Columns Columns
	From    From
	Where   Where
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

func (s *Select) Execute() (res *cdriver.DbResult, err error) {
	var table *cdriver.Table

	if len(s.From) < 1 {
		return nil, errors.New("FROM statement is empty")
	}

	table, err = getTable(s.From[0].Value)

	if err != nil {
		return nil, err
	}

	findConds := []cdriver.FindFieldCond{}

	for _, wc := range s.Where {
		findConds = append(findConds, cdriver.FindFieldCond{
			Field: wc.OperandA,
			Value: wc.OperandB,
		})
	}

	res, err = table.FindByIndex(findConds, s.Limit.RowCount, s.Limit.Offset)
	if err != nil {
		return nil, err
	}

	return res, nil
}