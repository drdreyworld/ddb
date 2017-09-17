package query

type Order []OrderExpr

type OrderExpr struct {
	Column    string
	Direction string
}