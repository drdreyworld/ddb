package qselect

import "ddb/structs/types"

type Order []OrderExpr

type OrderExpr struct {
	Column    string
	Direction string
}

func ParseOrder(q string) (query string, order Order, result bool) {
	if _, q, result = parseByRegexp(reorder, q); !result {
		return q, nil, result
	}

	order = Order{}

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

				order = append(order, OrderExpr{
					Column:    c.Value,
					Direction: string(direction),
				})
			} else {
				order = append(order, OrderExpr{
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

