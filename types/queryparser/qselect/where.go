package qselect

import (
	"regexp"
	"strings"
	"ddb/types/query"
)

type ParseWhereCondFunc func(q string) (qr string, whereCond query.WhereCond, result bool)

func ParseWhere(q string) (qr string, where query.Where, result bool) {

	where = query.Where{}

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
			var wc query.WhereCond

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

func ParseWhereOperand(q string) (qr string, operand string, result bool) {
	qr = q

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
			qr = string(o.ReplaceAll([]byte(q), []byte{}))
			result = true

			break
		}
	}

	return qr, operand, result
}

func ParseWhereCond(q string) (qr string, whereCond query.WhereCond, result bool) {

	qr = q

	if q, whereCond.OperandA, result = ParseWhereOperand(q); !result {
		return qr, whereCond, result
	}

	recompare := regexp.MustCompile(`^(?i)\s*(<>|<|>|=|!=|\s*in\s*){1}\s*`)

	if recompare.MatchString(q) {
		whereCond.Compartion = strings.Trim(recompare.FindString(q), " ")
		q = string(recompare.ReplaceAll([]byte(q), []byte{}))
	} else {
		return qr, whereCond, false
	}

	if q, whereCond.OperandB, result = ParseWhereOperand(q); !result {
		return qr, whereCond, result
	}

	return q, whereCond, result
}
