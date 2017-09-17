package storage

import "ddb/types/config"

type Storage interface {
	GetColumns()[]string
	GetRowsCount() int
	GetValue(position int, column string) interface{}
	GetBytes(position int, column string) []byte
	SetBytes(position int, column string, value []byte)

	GetRowStringMapByIndex(index int) map[string]string
	GetRowBytesByIndex(index int) map[string][]byte

	Init(tableName string, cfg config.ColumnsConfig)
	Load() error
	Save() error
}