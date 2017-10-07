package btree

import (
	"ddb/types"
	"ddb/types/key"
	"ddb/types/query"
	"ddb/types/storage"
	"fmt"
	"time"
	"os"
	"bufio"
	"ddb/types/funcs"
	"ddb/types/config"
)

type Index struct {
	Table   string
	Name    string
	Columns config.ColumnsConfig
	tree    BTree
}

func (i *Index) Init(Name, Table string) {
	i.Name = Name
	i.Table = Table
	i.tree.Degree = 200
}

func (i *Index) GetName() string {
	return i.Name
}

func (i *Index) GetColumns() config.ColumnsConfig {
	return i.Columns
}

func (i *Index) GetColumnNames() []string {
	result := []string{}
	for j := range i.Columns {
		result = append(result, i.Columns[j].Name)
	}
	return result
}

func (i *Index) SetColumns(columns config.ColumnsConfig) {
	i.Columns = columns
}

func (i *Index) Add(position int, columnsKeys map[string]key.BytesKey) {
	var value *Value
	var tree *BTree

	colCount := len(i.Columns)

	for j := range i.Columns {

		column := i.Columns[j].Name

		if value == nil {
			tree = &i.tree
		} else if tree = value.Tree; tree == nil {
			panic("Can't get subtree")
		}

		if value = tree.Find(columnsKeys[column]); value == nil {
			value = &Value{
				Key: columnsKeys[column],
			}

			if j < colCount - 1 {
				value.Tree = &BTree{
					Degree: i.tree.Degree,
				}
			}
			tree.Insert(value)
		}
	}

	if value.Data == nil {
		value.Data = []int{position}
	} else {
		value.Data = append(value.Data, position)
	}
}

func (i *Index) Set(positions []int, columnsKeys map[string]key.BytesKey) {
	var value *Value
	var tree *BTree

	colCount := len(i.Columns)

	for j := range i.Columns {

		column := i.Columns[j].Name

		if value == nil {
			tree = &i.tree
		} else if tree = value.Tree; tree == nil {
			panic("Can't get subtree")
		}

		if value = tree.Find(columnsKeys[column]); value == nil {
			if j < colCount - 1 {
				value = &Value{
					Key: columnsKeys[column],
					Tree: &BTree{
						Degree: i.tree.Degree,
					},
				}
			} else {
				value = &Value{
					Key: columnsKeys[column],
					Data: positions,
				}
			}
			tree.Insert(value)
		}
	}
}

func (i *Index) Traverse(orderColumns map[string]string, whereCallback func(column string, value key.BytesKey) bool, callback func(positions []int) bool) bool {

	var traverse func(tree *BTree, depth int) bool

	traverse = func(tree *BTree, depth int) bool {
		if tree.Root == nil {
			return true
		}

		column := i.Columns[depth].Name
		direction := orderColumns[column]

		if direction == "ASC" {
			return tree.Root.InfixTraverse(func(value *Value) bool {
				if whereCallback != nil {
					if !whereCallback(column, value.Key) {
						return true
					}
				}

				if value.Tree != nil {
					return traverse(value.Tree, depth+1)
				} else {
					return callback(value.Data)
				}
			})
		} else {
			return tree.Root.PostfixTraverse(func(value *Value) bool {
				if whereCallback != nil {
					if !whereCallback(column, value.Key) {
						return true
					}
				}

				if value.Tree != nil {
					return traverse(value.Tree, depth+1)
				} else {
					return callback(value.Data)
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
		indexColumns = i.GetColumnNames()
		allColumns = indexColumns
	} else {
		indexColumns, allColumns, ignoreColumns = i.GetColumnsForIndex(cond, order)

		// индекс не нужен
		if len(indexColumns) == 0 {
			return false
		}

		cols := map[string]string{}
		colsconf := config.ColumnsConfig{}

		for _, col := range indexColumns {
			cols[col] = col
		}

		if len(i.Columns) > 0 {
			for _, col := range i.Columns {
				if _, ok := cols[col.Name]; ok {
					colsconf = append(colsconf, col)
				}
			}
		} else {
			for _, col := range storage.GetColumnsConfig() {
				if _, ok := cols[col.Name]; ok {
					colsconf = append(colsconf, col)
				}
			}
		}
		i.SetColumns(colsconf)
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
	st := time.Now()
	fmt.Println("Load index table", i.Table, "idx", i.Name)
	fmt.Println("FileName:", i.GetFileName())

	f, err := os.OpenFile(i.GetFileName(), os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		return err
	}

	fileSize := int(s.Size())
	blockSize := 100 * 1024 * 1024;

	colCount := len(i.Columns)
	bytes := make([]byte, blockSize)
	row := map[string]key.BytesKey{}

	readMore := func(position, need, bytesCount int) (error, int, int) {
		for position + need > bytesCount {
			tail := bytes[position:bytesCount]
			bytes = make([]byte, blockSize)
			bytesCount, err = f.Read(bytes)
			fileSize -= bytesCount
			if err != nil {
				return err, position, bytesCount
			}
			bytesCount += len(tail)
			bytes = append(tail, bytes...)
			position = 0
		}
		//if position + need > bytesCount {
		//}
		return nil, position, bytesCount
	}

	columnLengths := 5 // count of positions

	for j := 0; j < colCount; j++ {
		columnLengths += 5 + i.Columns[j].Length
	}

	bytesCount := 0

	err, _, bytesCount = readMore(0, columnLengths, 0)
	if err != nil {
		return err
	}

	fileSize = int(s.Size())
	for p := 0; p < fileSize; {
		err, p, bytesCount = readMore(p, columnLengths, bytesCount)
		if err != nil {
			if err.Error() == "EOF" {
				err = nil
				break
			}
			return err
		}
		for j := 0; j < colCount; j++ {

			l := i.Columns[j].Length
			p+=5

			//fmt.Println("key", bytes[p : p+l])
			row[i.Columns[j].Name] = key.BytesKey(bytes[p : p+l])
			p += l
		}

		//fmt.Println("length", bytes[p : p+5])
		l := int(funcs.Int32FromBytes(bytes[p : p+5]))
		p += 5

		err, p, bytesCount = readMore(p, l * 5, bytesCount)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		positions := make([]int, l)
		for k := 0; k < l; k++ {
			positions[k] = int(funcs.Int32FromBytes(bytes[p : p+5]))
			p += 5
		}
		//fmt.Println("load positions:", positions)
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

	keys := make([][]byte, len(i.Columns))

	depth := 0

	var fn func(item *Item) bool

	fn = func(item *Item) bool {
		for i := 0; i < item.Count; i++ {
			value := item.Values[i]
			keys[depth] = value.Key
			if value.Tree != nil && value.Tree.Root != nil{
				depth++
				value.Tree.Root.InfixTraverseItems(fn)
				depth--
			} else if value.Data != nil {
				// save keys
				for j := 0; j < len(keys); j++ {
					w.Write(funcs.Int32ToBytes(int32(len(keys[j]))))
					w.Write(keys[j])
				}
				// save positions
				w.Write(funcs.Int32ToBytes(int32(len(value.Data))))
				//fmt.Println("save positions:", value.Data)
				for j := 0; j < len(value.Data); j++ {
					w.Write(funcs.Int32ToBytes(int32(value.Data[j])))
				}
			}
		}
		return true
	}

	i.tree.Root.InfixTraverseItems(fn)

	if err = w.Flush(); err != nil {
		panic(err)
		return err
	}

	fmt.Println("save finished:", time.Now().Sub(st))

	return err
}