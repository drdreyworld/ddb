package driver

type Condition struct {
	CompareA interface{}
	CompareB interface{}
	CompareFunc CompareFunc
}

type CompareFunc func(interface{}, interface{}) bool

func CompareEqualfunc(interface{}, interface{}) bool {
	return true
}