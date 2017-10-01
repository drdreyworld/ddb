package qinsert

import (
	"ddb/types/query"
	"strings"
)

func ParseColumns(q string) (qr string, columns query.Columns, result bool) {
	matches := recolumns.FindAllStringSubmatch(q, 2)
	if len(matches) == 0 || len(matches[0][1]) < 2 {
		return q, columns, false
	}

	qr = strings.Replace(q, matches[0][0], "", 1)
	matches[0] = strings.Split(matches[0][1], ",")

	for i := 0; i < len(matches[0]); i++ {
		columns = append(columns, query.Column(strings.Trim(matches[0][i], " ")))
	}

	return qr, columns, true
}