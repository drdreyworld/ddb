package engine

import "ddb/storage/config"

type Storage interface {
	GetColumnsConfig()config.ColumnsConfig
	GetColumns()[]string
	GetRowsCount() int

	GetValueByColumnIndex(position int, columnIndex int) interface{}
	GetBytesByColumnIndex(position int, columnIndex int) []byte

	GetBytes(position int, column string) []byte
	SetBytes(position int, column string, value []byte)

	GetRowStringMapByIndex(index int) map[string]string
	GetRowBytesByIndex(index int) map[string][]byte
	SetRowBytesByIndex(index int, row map[string][]byte)

	Open(tableName string, cfg config.ColumnsConfig)
	Close() error
}
