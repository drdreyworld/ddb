package qselect

import "strings"

type From []FromExpr

type FromType string

const (
	FROM_TYPE_TABLE FromType = "table"
)

type FromExpr struct {
	Type  FromType
	Value string
}

type ParseFromTableExprFunc func(q string) (query string, fromCond FromExpr, result bool)

func ParseFrom(q string) (query string, from From, result bool) {

	from = From{}

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
			var fe FromExpr
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

func ParseFromTableExpr(q string) (query string, fromExpr FromExpr, result bool) {

	match, q, result := parseByRegexp(refromtable, q)

	if result {
		fromExpr.Type = FROM_TYPE_TABLE
		fromExpr.Value = strings.Trim(match.(string), " ")
	}
	return q, fromExpr, result
}

