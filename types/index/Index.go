package index

import (
	"ddb/types"
	"ddb/types/storage"
	"ddb/types/query"
	"ddb/types/key"
	"ddb/types/config"
)

type Index interface {
	Init()

	SetTableName(table string)
	GetTableName() string

	GetName() string
	SetName(name string)

	GenerateName(prefix string)

	IsTemporary() bool
	SetTemporaryFlag(flag bool)

	Add(position int, columnsKeys map[string]key.BytesKey)

	GetColumns() config.ColumnsConfig
	SetColumns(columns config.ColumnsConfig)

	Traverse(orderColumns map[string]string, whereCallback func(column string, value key.BytesKey) bool, callback func(positions []int) bool) bool

	GetColumnsForIndex(columns config.ColumnsConfig, cond types.CompareConditions, order query.Order) config.ColumnsConfig

	BuildIndex(storage storage.Storage)

	Load() error
	Save() error
}

type Indexes []Index

func (i *Indexes) Load() {
	for ii := range (*i) {
		(*i)[ii].Load()
	}
}

func (i *Indexes) Save() {
	for ii := range (*i) {
		(*i)[ii].Save()
	}
}