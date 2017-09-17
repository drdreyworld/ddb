package query

type SelectExprs []SelectExpr

type SelectExprType string

const (
	SEL_EXPR_TYPE_COLUMN    SelectExprType = "column"
	SEL_EXPR_TYPE_FUNCTION  SelectExprType = "function"
	SEL_EXPR_TYPE_CONST_STR SelectExprType = "const_str"
	SEL_EXPR_TYPE_CONST_INT SelectExprType = "const_int"
	SEL_EXPR_TYPE_EXPR_MATH SelectExprType = "expr_math"
)

type SelectExpr struct {
	Type  SelectExprType
	Value string
}