package qselect

import (
	"strings"
	"ddb/types/query"
)

type ParseFromTableExprFunc func(q string) (query string, fromCond query.FromExpr, result bool)

func ParseFrom(q string) (qr string, from query.FromExprs, result bool) {

	from = query.FromExprs{}

	if _, q, result = parseByRegexp(refrom, q); !result {
		return q, from, result
	}

	funcs := []ParseFromTableExprFunc{
		ParseFromTableExpr,
	}

	result = false

	for {
		r := true

		if result && !recomma.MatchString(q) {
			break
		}

		q = string(recomma.ReplaceAll([]byte(q), []byte{}))

		for _, f := range funcs {
			var fe query.FromExpr
			if q, fe, r = f(q); !r {
				continue
			}
			from = append(from, fe)
			result = true
			break
		}

		if !r {
			break
		}
	}

	return q, from, result
}

func ParseFromTableExpr(q string) (qr string, fromExpr query.FromExpr, result bool) {

	match, q, result := parseByRegexp(refromtable, q)

	if result {
		fromExpr.Type = query.FROM_TYPE_TABLE
		fromExpr.Value = strings.Trim(match.(string), " ")
	}
	return q, fromExpr, result
}

