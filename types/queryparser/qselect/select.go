package qselect

import (
	"ddb/types/query"
	"errors"
)

func CreateSelectFromString(q string) (*query.Select, error) {
	var b bool

	if _, q, b = matchAndReplace(reselect, q); !b {
		return nil, nil
	}

	result := &query.Select{}

	q, result.Columns, b = ParseColumns(q)
	q, result.From, b = ParseFrom(q)
	q, result.Where, b = ParseWhere(q)
	q, result.Order, b = ParseOrder(q)
	q, result.Limit, b = ParseLimit(q)

	if len(q) > 0 {
		return nil, errors.New("parse error near " + q)
	}

	return result, nil
}
// @TODO
func SetConstants(result *query.Select, constants map[string]string) {
	for i := range result.Where {
		if val, ok := constants[result.Where[i].OperandB]; ok {
			result.Where[i].OperandB = val
		}
	}
}