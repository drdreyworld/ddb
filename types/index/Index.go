package index

import (
	"ddb/types"
	"ddb/types/storage"
	"ddb/types/query"
	"ddb/types/key"
)

type Index interface {
	Init(Name, Table string)

	Add(position int, columnsKeys map[string]key.BytesKey)

	GetColumns() []string

	SetColumns(columns []string)

	Traverse(orderColumns map[string]string, whereCallback func(column string, value interface{}) bool, callback func(positions []int) bool) bool

	GetColumnsForIndex(cond types.CompareConditions, order query.Order) ([]string, []string, map[string]bool)

	BuildIndex(storage storage.Storage, cond types.CompareConditions, order query.Order) bool

	Load() error
	Save() error
}

type WhereCallback func(column string, value []byte) bool

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