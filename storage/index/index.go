package index

import (
	"ddb/storage/config"
	"ddb/storage/engine"
)

type Indexes []Index

type ScanWhereFunc func(column string, value []byte) bool

type ScanRowFunc func(pos []int) bool

type Index interface {
	Open(cfg config.IndexConfig)
	Close()

	GetColumns() config.ColumnsConfig

	BuildIndex(storage engine.Storage)

	Insert(rowIndex int, rowData map[string][]byte)
	Delete(rowIndex int, rowData map[string][]byte)

	ScanRows(where ScanWhereFunc, fn ScanRowFunc, order map[string]string)
}
