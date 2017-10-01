package qinsert

import (
	"regexp"
	"strings"
	"ddb/types/query"
)

type ParseValueFuncs func(q string) (query string, exprs query.Value, result bool)

func ParseValues(q string) (qr string, values query.Values, result bool) {

	revaluesprefix := regexp.MustCompile(`^(?i)\s*values\s*`)
	if _, q, result = matchAndReplace(revaluesprefix, q); !result {
		return q, nil, result
	}

	funcs := []ParseValueFuncs{
		ParseColumnMath,
		ParseColumnConstStr,
		ParseColumnConstInt,
		ParseColumnFuncCall,
	}

	reobr := regexp.MustCompile(`^\s*\(\s*`)
	recbr := regexp.MustCompile(`^\s*\)\s*`)

	rowMatched := false

	for {
		if rowMatched && !recomma.MatchString(q) {
			break
		}

		q = string(recomma.ReplaceAll([]byte(q), []byte{}))

		if !reobr.MatchString(q) {
			break
		}

		q = string(reobr.ReplaceAll([]byte(q), []byte{}))

		rowMatched = false
		valueMatched := false

		valueRow := query.ValuesRow{}

		for {
			r := true

			if valueMatched && !recomma.MatchString(q) {
				break
			}

			q = string(recomma.ReplaceAll([]byte(q), []byte{}))

			for _, f := range funcs {
				var value query.Value

				if q, value, r = f(q); !r {
					continue
				}

				valueRow = append(valueRow, value)
				valueMatched = true
				break
			}

			if !r {
				break
			}
		}

		if !recbr.MatchString(q) {
			break
		}

		q = string(recbr.ReplaceAll([]byte(q), []byte{}))

		rowMatched = true

		values = append(values, valueRow)
	}

	return q, values, result
}

func parseInsertValueByRegexp(q string, re *regexp.Regexp, ctype string) (qr string, value query.Value, result bool) {
	match, q, result := parseByRegexp(re, q)
	if result {
		value.Type = ctype
		value.Data = strings.Trim(match.(string), " ")
	}
	return q, value, result
}

func ParseColumnMath(q string) (qr string, expr query.Value, result bool) {
	return parseInsertValueByRegexp(q, remath, query.INSERT_VALUE_MATH)
}

func ParseColumnConstStr(q string) (qr string, expr query.Value, result bool) {
	return parseInsertValueByRegexp(q, reconststr, query.INSERT_VALUE_MATH_CONST_STR)
}

func ParseColumnConstInt(q string) (qr string, expr query.Value, result bool) {
	return parseInsertValueByRegexp(q, reconstint, query.INSERT_VALUE_MATH_CONST_INT)
}

func ParseColumnFuncCall(q string) (qr string, expr query.Value, result bool) {
	return parseInsertValueByRegexp(q, refunc, query.INSERT_VALUE_MATH_FUNCTION)
}