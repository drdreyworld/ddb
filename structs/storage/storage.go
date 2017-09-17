package storage

type Storage interface {
	GetColumns()[]string
	GetRowsCount() int
	GetValue(position int, column string) interface{}
	GetBytes(position int, column string) []byte
	SetBytes(position int, column string, value []byte)
}
