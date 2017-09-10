package types

type QueryParser interface {
	Parse(q string) (Query, error)
}

type Query interface {
	Execute() (*Rowset, error)
}


