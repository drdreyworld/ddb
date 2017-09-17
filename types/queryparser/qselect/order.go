package qselect

import (
	"ddb/types/query"
)

func ParseOrder(q string) (qr string, order query.Order, result bool) {
	if _, q, result = parseByRegexp(reorder, q); !result {
		return q, nil, result
	}

	order = query.Order{}

	funcs := []ParseSelExprFuncs{
		ParseColumnCol,
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

			if reorderdir.MatchString(q) {
				direction := reorderdir.FindString(q)
				q = string(reorderdir.ReplaceAll([]byte(q), []byte{}))

				order = append(order, query.OrderExpr{
					Column:    c.Value,
					Direction: string(direction),
				})
			} else {
				order = append(order, query.OrderExpr{
					Column:    c.Value,
					Direction: "ASC",
				})
			}
			result = true
			break
		}

		if !r {
			break
		}
	}

	return q, order, result
}

