package qinsert

import (
	"ddb/types/query"
	"errors"
)

func CreateInsertFromString(q string) (*query.Insert, error) {
	var b bool

	if _, q, b = matchAndReplace(reinsert, q); !b {
		return nil, nil
	}

	result := &query.Insert{}

	q, result.Table, b = ParseTable(q)
	q, result.Columns, b = ParseColumns(q)
	q, result.Values, b = ParseValues(q)

	if len(q) > 0 {
		return nil, errors.New("parse error near " + q)
	}

	return result, nil
}

func SetConstants(insert *query.Insert, constants map[string]string) {
	for i := range insert.Values {
		for j := range insert.Values[i] {
			value := insert.Values[i][j]
			switch value.Type {
			case query.INSERT_VALUE_MATH_CONST_STR:
				if val, ok := constants[value.Data]; ok {
					value.Data = val
				} else {
					panic("Constant not registered " + value.Data)
				}
				insert.Values[i][j] = value
				break;
			}
		}
	}
}