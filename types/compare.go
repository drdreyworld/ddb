package types

type Key []byte

const (
	CMP_KEY_EQUAL    = 0
	CMP_KEY_LESS     = -1
	CMP_KEY_GREATHER = 1
)

func (k Key) Equal(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_EQUAL
}

func (k Key) Less(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_LESS
}

func (k Key) Greather(key Key) (r bool) {
	return k.Compare(key) == CMP_KEY_GREATHER
}

func (k Key) Compare(key Key) (r int) {
	lk := len(k)
	lK := len(key)

	l := lk

	if lK > l {
		l = lK
	}

	var a, b byte

	for i := 0; i < l; i++ {
		if i >= lk {
			a = 0
		} else {
			a = (k)[i]
		}

		if i >= lK {
			b = 0
		} else {
			b = key[i]
		}

		if a < b {
			return CMP_KEY_LESS
		} else if a > b {
			return CMP_KEY_GREATHER
		}
	}

	return CMP_KEY_EQUAL
}


type CompareFunc func(OperandA Key, OperandB []byte) bool

func CompareEqual(OperandA Key, OperandB []byte) bool {
	return OperandA.Equal(OperandB)
}

func CompareNotEqual(OperandA Key, OperandB []byte) bool {
	return !OperandA.Equal(OperandB)
}

func CompareLess(OperandA Key, OperandB []byte) bool {
	return OperandA.Less(OperandB)
}

func CompareGreather(OperandA Key, OperandB []byte) bool {
	return OperandA.Greather(OperandB)
}

type CompareConditions []CompareCondition

func (c *CompareConditions) ByColumnName(name string) (result CompareConditions) {
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

func (wc *CompareCondition) GetCompareFunc() CompareFunc {
	switch wc.Compartion {
	case "=":
		return CompareEqual
	case "<>":
		return CompareNotEqual
	case "!=":
		return CompareNotEqual
	case "<":
		return CompareLess
	case ">":
		return CompareGreather
	}

	return nil
}

func (wc *CompareCondition) Compare(value []byte) bool {
	return wc.GetCompareFunc()(value, wc.Value)
}


