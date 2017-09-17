package mbbtree

import (
	"bufio"
	"ddb/types"
	"ddb/types/query"
	"ddb/types/storage"
	"fmt"
	"os"
	"strconv"
	"time"
	"ddb/types/funcs"
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
}

func (i *Index) GetColumns() []string {
	return i.Columns
}

func (i *Index) SetColumns(columns []string) {
	i.Columns = columns
}

func (i *Index) Add(position int, columnsKeys map[string]interface{}) {
	var item *TItem

	tree := &i.tree

	for _, column := range i.Columns {
		key := columnsKeys[column].([]byte)

		if item = tree.Find(key); item == nil {
			tree.Add(NewData(key, nil))
			item = tree.Find(key)
			tree = item.GetSubTree()
		} else {
			tree = item.GetSubTree()
		}
	}

	if positions, ok := item.GetValue().([]int); ok {
		if !posInSlice(position, positions) {
			positions = append(positions, position)
			item.SetValue(positions)
		}
	} else {
		item.SetValue([]int{position})
	}
}

func (i *Index) Set(positions []int, columnsKeys map[string]interface{}) {
	var item *TItem

	tree := &i.tree

	for _, column := range i.Columns {
		key := columnsKeys[column].([]byte)

		if item = tree.Find(key); item == nil {
			tree.Add(NewData(key, nil))
			item = tree.Find(key)
			tree = item.GetSubTree()
		} else {
			tree = item.GetSubTree()
		}
	}

	item.SetValue(positions)
}

func (i *Index) Traverse(orderColumns map[string]string, whereCallback func(column string, value []byte) bool, callback func(positions []int) bool) bool {

	var traverse func(tree *BTree, depth int) bool

	traverse = func(tree *BTree, depth int) bool {
		if tree.Root() == nil {
			return true
		}

		column := i.Columns[depth]
		direction := orderColumns[column]

		if direction == "ASC" {
			return tree.Root().InfixTraverse(func(i *TItem) bool {
				if whereCallback != nil {
					if !whereCallback(column, i.GetKey()) {
						return true
					}
				}

				if i.GetSubTree() != nil && i.GetSubTree().Root() != nil {
					return traverse(i.GetSubTree(), depth+1)
				} else {
					if pos, ok := i.GetValue().([]int); ok {
						if !callback(pos) {
							return false
						}
					} else {
						panic("Item get value type conversion error")
					}
				}
				return true
			})
		} else {
			return tree.Root().PostfixTraverse(func(i *TItem) bool {
				if whereCallback != nil {
					if !whereCallback(column, i.GetKey()) {
						return true
					}
				}

				if i.GetSubTree() != nil && i.GetSubTree().Root() != nil {
					return traverse(i.GetSubTree(), depth+1)
				} else {
					if pos, ok := i.GetValue().([]int); ok {
						if !callback(pos) {
							return false
						}
					} else {
						panic("Item get value type conversion error")
					}
				}
				return true
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

		if _, ok := indexColumnsMap[column]; !ok {
			indexColumnsMap[column] = true
			indexColumns = append(indexColumns, column)
		}
	}

	return indexColumns, allColumns, ignoreColumns
}

func (i *Index) BuildIndex(storage storage.Storage, cond types.CompareConditions, order query.Order) {

	allColumns := []string{}

	indexColumns := []string{}
	ignoreColumns := map[string]bool{}

	if len(cond) == 0 && len(order) == 0 {
		indexColumns = i.Columns
		allColumns = indexColumns
	} else {
		indexColumns, allColumns, ignoreColumns = i.GetColumnsForIndex(cond, order)
		i.SetColumns(indexColumns)
	}

	positions := map[string][]int{}
	rows := map[string]map[string]interface{}{}

	for position := 0; position < storage.GetRowsCount(); position++ {
		matched := true

		key := ""
		row := map[string]interface{}{}
		for _, columnName := range allColumns {

			bytes := storage.GetBytes(position, columnName)

			conditions := cond.ByColumnName(columnName)
			for _, condition := range conditions {
				matched = matched && condition.Compare(bytes)
				if !matched {
					break
				}
			}

			if _, ok := ignoreColumns[columnName]; !ok {
				row[columnName] = bytes
				value := storage.GetValue(position, columnName)
				switch value.(type) {
				case string:
					key += columnName + ":" + value.(string)
					break
				case int32:
					key += columnName + ":" + strconv.Itoa(int(value.(int32)))
					break
				default:
					panic("Unknown value type")
				}
			}
		}

		if matched {
			if ps, ok := positions[key]; ok {
				positions[key] = append(ps, position)
			} else {
				positions[key] = []int{position}
				rows[key] = row
			}
		}
	}

	for key := range rows {
		i.Set(positions[key], rows[key])
	}
}

func (i *Index) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + i.Table + ".idx" + i.Name
}

func (i *Index) Load() error {
	st := time.Now()
	fmt.Println("Load index table", i.Table, "idx", i.Name)
	fmt.Println("FileName:", i.GetFileName())

	bytes := []byte{}

	f, err := os.OpenFile(i.GetFileName(), os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		return err
	}

	bytes = make([]byte, s.Size())
	if _, err := f.Read(bytes); err != nil {
		return err
	}

	for p := 0; p < len(bytes); {
		row := map[string]interface{}{}

		for j := 0; j < len(i.Columns); j++ {

			l := int(funcs.Int32FromBytes(bytes[p:p+5]))
			p += 5

			row[i.Columns[j]] = bytes[p:p+l]
			p += l

			//fmt.Println("column", j, ",", i.Columns[j], ":", funcs.StringFromNullByte(row[i.Columns[j]].([]byte)))
		}
		l := int(funcs.Int32FromBytes(bytes[p:p+5]))
		p+=5
		//positions := make([]int, l)
		positions := []int{}
		for k := 0; k < l; k++ {
			positions = append(positions, int(funcs.Int32FromBytes(bytes[p:p+5])))
			p+=5
		}
		i.Set(positions, row)
	}

	fmt.Println("load finished:", time.Now().Sub(st))

	return err
}

func (i *Index) Save() error {
	st := time.Now()
	fmt.Println("Save index table", i.Table, "idx", i.Name)
	fmt.Println("FileName:", i.GetFileName())

	f, err := os.OpenFile(i.GetFileName(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	colpos := map[string]int{}

	for i, col := range i.Columns {
		colpos[col] = i
	}

	row := make([][]byte, len(i.Columns))

	i.Traverse(
		map[string]string{},
		func(column string, value []byte) bool {
			row[colpos[column]] = value
			return true
		},
		func(positions []int) bool {
			for j := 0; j < len(row); j++ {
				w.Write(funcs.Int32ToBytes(int32(len(row[j]))))
				w.Write(row[j])
			}
			w.Write(funcs.Int32ToBytes(int32(len(positions))))
			for i := 0; i < len(positions); i++ {
				w.Write(funcs.Int32ToBytes(int32(positions[i])))
			}
			return true
		},
	)

	if err = w.Flush(); err != nil {
		panic(err)
		return err
	}

	fmt.Println("save finished:", time.Now().Sub(st))

	return err
}

func posInSlice(pos int, ps []int) bool {
	for _, v := range ps {
		if v == pos {
			return true
		}
	}
	return false
}
