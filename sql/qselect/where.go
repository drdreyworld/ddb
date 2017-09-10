package qselect

import (
	"regexp"
	"strings"
)

type ParseWhereCondFunc func(q string) (query string, whereCond WhereCond, result bool)

type Where []WhereCond

type WhereCond struct {
	OperandA   string
	OperandB   string
	Compartion string
}

func ParseWhere(q string) (query string, where Where, result bool) {

	where = Where{}

	if _, q, result = parseByRegexp(rewhere, q); !result {
		return q, where, result
	}

	funcs := []ParseWhereCondFunc{
		ParseWhereCond,
	}

	result = false

	for {
		r := true

		if result && !reandor.MatchString(q) {
			break
		}

		q = string(reandor.ReplaceAll([]byte(q), []byte{}))

		for _, f := range funcs {
			var wc WhereCond

			if q, wc, r = f(q); !r {
				continue
			}

			where = append(where, wc)

			result = true
			break
		}

		if !r {
			break
		}
	}

	return q, where, result
}

func ParseWhereOperand(q string) (query string, operand string, result bool) {
	query = q

	operands := []*regexp.Regexp{
		reconstint,
		reconststr,
		refunc,
		recolumn,
		reenum,
	}

	for _, o := range operands {
		if o.MatchString(q) {
			operand = strings.Trim(o.FindString(q), " ")
			query = string(o.ReplaceAll([]byte(q), []byte{}))
			result = true

			break
		}
	}

	return query, operand, result
}

func ParseWhereCond(q string) (query string, whereCond WhereCond, result bool) {

	query = q

	if q, whereCond.OperandA, result = ParseWhereOperand(q); !result {
		return query, whereCond, result
	}

	recompare := regexp.MustCompile(`^(?i)\s*(<|>|=|\s*in\s*)\s*`)

	if recompare.MatchString(q) {
		whereCond.Compartion = recompare.FindString(q)
		q = string(recompare.ReplaceAll([]byte(q), []byte{}))
	} else {
		return query, whereCond, false
	}

	if q, whereCond.OperandB, result = ParseWhereOperand(q); !result {
		return query, whereCond, result
	}

	return q, whereCond, result
}