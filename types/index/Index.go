package index

import (
	"ddb/types"
	"ddb/types/storage"
	"ddb/types/query"
	"ddb/types/key"
	"ddb/types/config"
)

type Index interface {
	Init(Name, Table string)
	GetName() string
	SetName(name string)

	IsTemporary() bool
	SetTemporaryFlag(flag bool)

	Add(position int, columnsKeys map[string]key.BytesKey)

	GetColumns() config.ColumnsConfig
	SetColumns(columns config.ColumnsConfig)

	Traverse(orderColumns map[string]string, whereCallback func(column string, value key.BytesKey) bool, callback func(positions []int) bool) bool

	GetColumnsForIndex(cond types.CompareConditions, order query.Order) ([]string, []string, map[string]bool)

	BuildIndex(storage storage.Storage, cond types.CompareConditions, order query.Order) bool

	Load() error
	Save() error
}

type WhereCallback func(column string, value key.BytesKey) bool

type TraverseCallback func(positions []int) bool

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