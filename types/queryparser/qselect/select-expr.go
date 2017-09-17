package qselect

import (
	"regexp"
	"strings"
	"ddb/types/query"
)

type ParseSelExprFuncs func(q string) (query string, exprs query.SelectExpr, result bool)

func ParseColumns(q string) (qr string, columns query.SelectExprs, result bool) {

	columns = query.SelectExprs{}

	funcs := []ParseSelExprFuncs{
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
			var c query.SelectExpr

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

func parseSelExprByRegexp(q string, re *regexp.Regexp, ct query.SelectExprType) (qr string, expr query.SelectExpr, result bool) {
	match, q, result := parseByRegexp(re, q)
	if result {
		expr.Type = ct
		expr.Value = strings.Trim(match.(string), " ")
	}
	return q, expr, result
}

func ParseColumnMath(q string) (qr string, expr query.SelectExpr, result bool) {
	return parseSelExprByRegexp(q, remath, query.SEL_EXPR_TYPE_EXPR_MATH)
}

func ParseColumnConstStr(q string) (qr string, expr query.SelectExpr, result bool) {
	return parseSelExprByRegexp(q, reconststr, query.SEL_EXPR_TYPE_CONST_STR)
}

func ParseColumnConstInt(q string) (qr string, expr query.SelectExpr, result bool) {
	return parseSelExprByRegexp(q, reconstint, query.SEL_EXPR_TYPE_CONST_INT)
}

func ParseColumnFuncCall(q string) (qr string, expr query.SelectExpr, result bool) {
	return parseSelExprByRegexp(q, refunc, query.SEL_EXPR_TYPE_FUNCTION)
}

func ParseColumnColWithAlias(q string) (qr string, expr query.SelectExpr, result bool) {
	// @TODO Parse column alias
	return parseSelExprByRegexp(q, recolumnWithAlias, query.SEL_EXPR_TYPE_COLUMN)
}

func ParseColumnCol(q string) (qr string, expr query.SelectExpr, result bool) {
	return parseSelExprByRegexp(q, recolumn, query.SEL_EXPR_TYPE_COLUMN)
}
