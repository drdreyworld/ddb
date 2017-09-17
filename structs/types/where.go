package types

import (
)

type Where []WhereCond

type WhereCond struct {
	OperandA   string
	OperandB   string
	Compartion string
}
//
//func (w *Where) CreateFindCond(wc *Columns) (res CompareConditions, err error) {
//	for i := range *w {
//		c := t.Columns.ByName(where[i].OperandA)
//		if c == nil {
//			return nil, errors.New("Unknown column " + where[i].OperandA)
//		}
//
//		switch c.Type {
//
//		case "int64":
//			val, err := strconv.Atoi(where[i].OperandB)
//			if err != nil {
//				return nil, err
//			}
//
//			ival, err := ValueToBytes(val, c.Length)
//			if err != nil {
//				return nil, err
//			}
//
//			res = append(res, types.CompareCondition{
//				Field:      where[i].OperandA,
//				Value:      ival,
//				Compartion: where[i].Compartion,
//			})
//			break
//
//		case "string":
//			sval, err := ValueToBytes(where[i].OperandB, c.Length)
//			if err != nil {
//				return nil, err
//			}
//
//			res = append(res, types.CompareCondition{
//				Field:      where[i].OperandA,
//				Value:      sval,
//				Compartion: where[i].Compartion,
//			})
//
//			break
//		default:
//			return nil, errors.New("Unknown column type " + c.Type)
//		}
//	}
//	return res, nil
//}