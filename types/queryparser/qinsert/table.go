package qinsert

import (
	"ddb/types/query"
	"strings"
)

func ParseTable(q string) (qr string, table query.Table, result bool) {
	var match interface{}
	if match, q, result = parseByRegexp(retable, q); !result {
		return q, table, result
	}
	return q, query.Table(strings.Trim(match.(string), " ")), result
}
