package qselect

import (
	"regexp"
	"strings"
)

type Columns []Column

type ColumnType string

const (
	COL_TYPE_COLUMN    ColumnType = "column"
	COL_TYPE_FUNCTION  ColumnType = "function"
	COL_TYPE_CONST_STR ColumnType = "const_str"
	COL_TYPE_CONST_INT ColumnType = "const_int"
	COL_TYPE_EXPR_MATH ColumnType = "expr_math"
)

type Column struct {
	Type  ColumnType
	Value string
}

type ParseColumnFunc func(q string) (query string, column Column, result bool)

func ParseColumns(q string) (query string, columns Columns, result bool) {

	columns = Columns{}

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
			var c Column

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

func parseColumnByRegexp(q string, re *regexp.Regexp, ct ColumnType) (query string, column Column, result bool) {
	match, q, result := parseByRegexp(re, q)
	if result {
		column.Type = ct
		column.Value = strings.Trim(match.(string), " ")
	}
	return q, column, result
}

func ParseColumnMath(q string) (query string, column Column, result bool) {
	return parseColumnByRegexp(q, remath, COL_TYPE_EXPR_MATH)
}

func ParseColumnConstStr(q string) (query string, column Column, result bool) {
	return parseColumnByRegexp(q, reconststr, COL_TYPE_CONST_STR)
}

func ParseColumnConstInt(q string) (query string, column Column, result bool) {
	return parseColumnByRegexp(q, reconstint, COL_TYPE_CONST_INT)
}

func ParseColumnFuncCall(q string) (query string, column Column, result bool) {
	return parseColumnByRegexp(q, refunc, COL_TYPE_FUNCTION)
}

func ParseColumnColWithAlias(q string) (query string, column Column, result bool) {
	// @TODO Parse column alias
	return parseColumnByRegexp(q, recolumnWithAlias, COL_TYPE_COLUMN)
}

func ParseColumnCol(q string) (query string, column Column, result bool) {
	return parseColumnByRegexp(q, recolumn, COL_TYPE_COLUMN)
}
