package qselect

import (
	"regexp"
	"strings"
	"ddb/structs/types"
	"fmt"
)

type ParseWhereCondFunc func(q string) (query string, whereCond types.WhereCond, result bool)

func ParseWhere(q string) (query string, where types.Where, result bool) {

	where = types.Where{}

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
			var wc types.WhereCond

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

func ParseWhereCond(q string) (query string, whereCond types.WhereCond, result bool) {

	query = q

	if q, whereCond.OperandA, result = ParseWhereOperand(q); !result {
		return query, whereCond, result
	}

	recompare := regexp.MustCompile(`^(?i)\s*(<>|\<|\>|=|!=|\s*in\s*){1}\s*`)

	if recompare.MatchString(q) {
		whereCond.Compartion = strings.Trim(recompare.FindString(q), " ")
		fmt.Println("parse compartion : ",whereCond.Compartion)
		q = string(recompare.ReplaceAll([]byte(q), []byte{}))
	} else {
		return query, whereCond, false
	}

	if q, whereCond.OperandB, result = ParseWhereOperand(q); !result {
		return query, whereCond, result
	}

	return q, whereCond, result
}
