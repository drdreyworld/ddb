package query

type Table string

type Columns []Column

type Column string

type Values []ValuesRow

type ValuesRow []Value

type Value struct {
	Type string
	Data string
}

const (
	INSERT_VALUE_MATH           = "math"
	INSERT_VALUE_MATH_CONST_STR = "const_str"
	INSERT_VALUE_MATH_CONST_INT = "const_int"
	INSERT_VALUE_MATH_FUNCTION  = "func"
)

type Insert struct {
	Table   Table
	Columns Columns
	Values  Values
}
