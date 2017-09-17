package query

type FromExprs []FromExpr

type FromType string

const (
	FROM_TYPE_TABLE FromType = "table"
)

type FromExpr struct {
	Type  FromType
	Value string
}
