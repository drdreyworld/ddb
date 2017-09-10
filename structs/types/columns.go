package types

type Columns []Column

type ColumnType string

const (
	COL_TYPE_COLUMN    ColumnType = "column"
	COL_TYPE_FUNCTION  ColumnType = "function"
	COL_TYPE_CONST_STR ColumnType = "const_str"
	COL_TYPE_CONST_INT ColumnType = "const_int"
	COL_TYPE_EXPR_MATH ColumnType = "expr_math"
)

type Column struct {
	Type  ColumnType
	Value string
}