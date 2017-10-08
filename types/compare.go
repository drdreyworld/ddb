package types

import "ddb/types/key"

type CompareConditions []CompareCondition

type CompareConditionsMap map[string]CompareConditions

func (c *CompareConditions) GroupByColumns() (result CompareConditionsMap) {
	result = make(CompareConditionsMap)

	for i := range (*c) {
		result[(*c)[i].Field] = append(result[(*c)[i].Field], (*c)[i])
	}

	return result
}

func (c *CompareConditions) ForColumn(name string) (result CompareConditions) {
	for i := range (*c) {
		if (*c)[i].Field == name {
			result = append(result, (*c)[i])
		}
	}

	return result
}

type CompareCondition struct {
	Field      string
	Value      []byte
	Compartion string
}

func (wc *CompareCondition) Compare(value key.BytesKey) bool {
	switch wc.Compartion {
	case "=":
		return value.Equal(wc.Value)
	case "<>", "!=":
		return !value.Equal(wc.Value)
	case "<":
		return value.Less(wc.Value)
	case "<=":
		return value.LessOrEqual(wc.Value)
	case ">":
		return value.Greather(wc.Value)
	case ">=":
		return value.GreatherOrEqual(wc.Value)
	}
	panic("Unknown compartion value " + wc.Compartion)
}


