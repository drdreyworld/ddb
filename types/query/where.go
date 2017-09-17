package query

type Where []WhereCond

type WhereCond struct {
	OperandA   string
	OperandB   string
	Compartion string
}