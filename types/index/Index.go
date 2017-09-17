package index

import (
	"ddb/types"
	"ddb/types/storage"
	"ddb/types/query"
)

type Index interface {
	Add(position int, columnsKeys map[string]interface{})

	Set(positions []int, columnsKeys map[string]interface{})

	GetColumns() []string

	SetColumns(columns []string)

	Traverse(orderColumns map[string]string, whereCallback func(column string, value []byte) bool, callback func(positions []int) bool) bool

	GetColumnsForIndex(cond types.CompareConditions, order query.Order) ([]string, []string, map[string]bool)

	BuildIndex(storage storage.Storage, cond types.CompareConditions, order query.Order)
}

type WhereCallback func(column string, value []byte) bool

type TraverseCallback func(positions []int) bool

type Indexes []Index
