package qselect

import (
	"regexp"
	"strings"
	"ddb/structs/types"
)

type ParseColumnFunc func(q string) (query string, column types.Column, result bool)

func ParseColumns(q string) (query string, columns types.Columns, result bool) {

	columns = types.Columns{}

	funcs := []ParseColumnFunc{
		ParseColumnMath,
		ParseColumnConstStr,
		ParseColumnConstInt,
		ParseColumnFuncCall,
		ParseColumnColWithAlias,
	}

	result = false

	for {
		r := true

		if result && !recomma.MatchString(q) {
			break
		}

		q = string(recomma.ReplaceAll([]byte(q), []byte{}))

		for _, f := range funcs {
			var c types.Column

			if q, c, r = f(q); !r {
				continue
			}

			columns = append(columns, c)
			result = true
			break
		}

		if !r {
			break
		}
	}

	return q, columns, result
}

func parseColumnByRegexp(q string, re *regexp.Regexp, ct types.ColumnType) (query string, column types.Column, result bool) {
	match, q, result := parseByRegexp(re, q)
	if result {
		column.Type = ct
		column.Value = strings.Trim(match.(string), " ")
	}
	return q, column, result
}

func ParseColumnMath(q string) (query string, column types.Column, result bool) {
	return parseColumnByRegexp(q, remath, types.COL_TYPE_EXPR_MATH)
}

func ParseColumnConstStr(q string) (query string, column types.Column, result bool) {
	return parseColumnByRegexp(q, reconststr, types.COL_TYPE_CONST_STR)
}

func ParseColumnConstInt(q string) (query string, column types.Column, result bool) {
	return parseColumnByRegexp(q, reconstint, types.COL_TYPE_CONST_INT)
}

func ParseColumnFuncCall(q string) (query string, column types.Column, result bool) {
	return parseColumnByRegexp(q, refunc, types.COL_TYPE_FUNCTION)
}

func ParseColumnColWithAlias(q string) (query string, column types.Column, result bool) {
	// @TODO Parse column alias
	return parseColumnByRegexp(q, recolumnWithAlias, types.COL_TYPE_COLUMN)
}

func ParseColumnCol(q string) (query string, column types.Column, result bool) {
	return parseColumnByRegexp(q, recolumn, types.COL_TYPE_COLUMN)
}
