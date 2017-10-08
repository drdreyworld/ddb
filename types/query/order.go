package query

type Order []OrderExpr

type OrderExpr struct {
	Column    string
	Direction string
}

func (order *Order) GetOrderMap() map[string]string {
	result := map[string]string{}
	for _, item := range *order {
		result[item.Column] = item.Direction
	}
	return result
}