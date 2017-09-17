package types

import "ddb/structs/storage"

type IndexConfig struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Columns []string `json:"columns"`
}

type Index interface {
	Create() Index

	Add(position int, columnsKeys map[string]interface{})

	Set(positions []int, columnsKeys map[string]interface{})

	GetColumns() []string

	SetColumns(columns []string)

	Traverse(columnsOrder map[string]string, whereCallback WhereCallback, callback TraverseCallback) bool

	GetColumnsForIndex(cond CompareConditions, order Order) ([]string, []string, map[string]bool)

	BuildIndex(storage storage.Storage, cond CompareConditions, order Order) Index
}

type IndexItem interface{}

type WhereCallback func(column string, value []byte) bool

type TraverseCallback func(positions []int) bool
