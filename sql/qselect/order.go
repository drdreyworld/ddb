package qselect

import (
	"ddb/structs/types"
)

func ParseOrder(q string) (query string, order types.Order, result bool) {
	if _, q, result = parseByRegexp(reorder, q); !result {
		return q, nil, result
	}

	order = types.Order{}

	funcs := []ParseColumnFunc{
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

			var c types.Column

			if q, c, r = f(q); !r {
				continue
			}

			if reorderdir.MatchString(q) {
				direction := reorderdir.FindString(q)
				q = string(reorderdir.ReplaceAll([]byte(q), []byte{}))

				order = append(order, types.OrderExpr{
					Column:    c.Value,
					Direction: string(direction),
				})
			} else {
				order = append(order, types.OrderExpr{
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

