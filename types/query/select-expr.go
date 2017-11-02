package query

import "ddb/storage/config"

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

func (exprs *SelectExprs) PrepareColumns(cc config.ColumnsConfig) {
	// select * magic >>>
	cols := SelectExprs{}
	selallcols := false

	for i := range *exprs {
		if (*exprs)[i].Value == "*" {
			if !selallcols {
				for _, col := range cc {
					cols = append(cols, SelectExpr{
						Value: col.Name,
						Type: SEL_EXPR_TYPE_COLUMN,
					})
				}
				selallcols = true
			} else {
				continue;
			}
		} else {
			cols = append(cols, (*exprs)[i])
		}
	}

	(*exprs) = cols
	// <<< select * magic
}