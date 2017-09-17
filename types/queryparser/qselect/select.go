package qselect

import (
	"ddb/types/query"
)

func CreateSelectFromString(q string) *query.Select {
	var b bool

	if _, q, b = matchAndReplace(reselect, q); !b {
		return nil
	}

	result := &query.Select{}

	q, result.Columns, b = ParseColumns(q)
	q, result.From, b = ParseFrom(q)
	q, result.Where, b = ParseWhere(q)
	q, result.Order, b = ParseOrder(q)
	q, result.Limit, b = ParseLimit(q)

	return result
}