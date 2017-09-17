package query

type Select struct {
	Columns SelectExprs
	From    FromExprs
	Where   Where
	Order   Order
	Limit   Limit
}