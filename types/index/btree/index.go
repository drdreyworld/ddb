package btree

import (
	"ddb/types"
	"ddb/types/key"
	"ddb/types/query"
	"ddb/types/storage"
)

type Index struct {
	Table   string
	Name    string
	Columns []string
	tree    BTree
}

func (i *Index) Init(Name, Table string) {
	i.Name = Name
	i.Table = Table
	i.tree.Degree = 200
}

func (i *Index) GetColumns() []string {
	return i.Columns
}

func (i *Index) SetColumns(columns []string) {
	i.Columns = columns
}

func (i *Index) Add(position int, columnsKeys map[string]key.BytesKey) {
	var value *Value
	var ok bool
	var tree *BTree

	for _, column := range i.Columns {

		if value == nil {
			tree = &i.tree
		} else if tree, ok = value.Data.(*BTree); !ok {
			panic("Can't get subtree")
		}

		if value = tree.Find(columnsKeys[column]); value == nil {
			value = &Value{
				Key: columnsKeys[column],
				Data: &BTree{
					Degree: i.tree.Degree,
				},
			}
			tree.Insert(value)
		}
	}

	if value.Data == nil {
		value.Data = []int{position}
	} else if positions, ok := value.Data.([]int); ok {
		value.Data = append(positions, position)
	} else {
		value.Data = []int{position}
	}
}

func (i *Index) Traverse(orderColumns map[string]string, whereCallback func(column string, value interface{}) bool, callback func(positions []int) bool) bool {

	var traverse func(tree *BTree, depth int) bool

	traverse = func(tree *BTree, depth int) bool {
		if tree.Root == nil {
			return true
		}

		column := i.Columns[depth]
		direction := orderColumns[column]

		if direction == "ASC" {
			return tree.Root.InfixTraverse(func(value *Value) bool {
				if whereCallback != nil {
					if !whereCallback(column, value.Key) {
						return true
					}
				}

				switch v := value.Data.(type) {
				case *BTree:
					return traverse(v, depth+1)
				case []int:
					if !callback(v) {
						return false
					}
					return true
				default:
					panic("unknown value Data type")
				}
			})
		} else {
			return tree.Root.PostfixTraverse(func(value *Value) bool {
				if whereCallback != nil {
					if !whereCallback(column, value.Key) {
						return true
					}
				}

				switch v := value.Data.(type) {
				case *BTree:
					return traverse(v, depth+1)
				case []int:
					if !callback(v) {
						return false
					}
					return true
				default:
					panic("unknown value Data type")
				}
			})
		}
	}

	return traverse(&i.tree, 0)
}

func (i *Index) GetColumnsForIndex(cond types.CompareConditions, order query.Order) ([]string, []string, map[string]bool) {
	allColumns := []string{}

	indexColumns := []string{}
	ignoreColumns := map[string]bool{}

	indexColumnsMap := map[string]bool{}
	allColumnsMap := map[string]bool{}

	for i := range order {
		column := order[i].Column
		if _, ok := indexColumnsMap[column]; !ok {
			indexColumnsMap[column] = true
			indexColumns = append(indexColumns, column)
		}
		if _, ok := allColumnsMap[column]; !ok {
			allColumnsMap[column] = true
			allColumns = append(allColumns, column)
		}
	}

	for i := range cond {
		column := cond[i].Field

		if _, ok := allColumnsMap[column]; !ok {
			allColumnsMap[column] = true
			allColumns = append(allColumns, column)
		}

		if cond[i].Compartion == "=" {
			if _, ok := indexColumnsMap[column]; ok {
				ignoreColumns[column] = true
				delete(indexColumnsMap, column)
				for j := range indexColumns {
					if indexColumns[j] == column {
						indexColumns = append(
							indexColumns[0:j],
							indexColumns[j+1:]...,
						)
						break
					}
				}
			}
			continue
		}
	}

	return indexColumns, allColumns, ignoreColumns
}

func (i *Index) BuildIndex(storage storage.Storage, cond types.CompareConditions, order query.Order) bool {

	allColumns := []string{}

	indexColumns := []string{}
	ignoreColumns := map[string]bool{}

	if len(cond) == 0 && len(order) == 0 {
		indexColumns = i.Columns
		allColumns = indexColumns
	} else {
		indexColumns, allColumns, ignoreColumns = i.GetColumnsForIndex(cond, order)

		// индекс не нужен
		if len(indexColumns) == 0 {
			return false
		}

		i.SetColumns(indexColumns)
	}

	rowsCount := storage.GetRowsCount()

	row := map[string]key.BytesKey{}

	conds := cond.GroupByColumns()

	for position := 0; position < rowsCount; position++ {
		matched := true

		for _, columnName := range allColumns {
			bytes := storage.GetBytes(position, columnName)

			for _, condition := range conds[columnName] {
				matched = matched && condition.Compare(bytes)
				if !matched {
					break
				}
			}

			if _, ok := ignoreColumns[columnName]; !ok {
				row[columnName] = bytes
			}
		}

		if matched {
			i.Add(position, row)
		}
	}
	return true
}

func (i *Index) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + i.Table + ".idx" + i.Name
}

func (i *Index) Load() error {
	return nil
}

func (i *Index) Save() error {
	return nil
}
