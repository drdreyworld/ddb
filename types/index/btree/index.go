package btree

import (
	"bufio"
	"ddb/types"
	"ddb/types/config"
	"ddb/types/key"
	"ddb/types/query"
	"ddb/types/storage"
	"fmt"
	"os"
	"time"
	"github.com/drdreyworld/smconv"
)

type Index struct {
	Table     string
	Name      string
	Columns   config.ColumnsConfig
	tree      BTree
	temporary bool
}

func (i *Index) Init() {
	i.tree.Degree = 200
}

func (i *Index) SetTableName(table string) {
	i.Table = table
}

func (i *Index) GetTableName() string {
	return i.Table
}

func (i *Index) GetName() string {
	return i.Name
}

func (i *Index) SetName(name string) {
	i.Name = name
}

func (i *Index) GenerateName(prefix string) {
	for j := range i.Columns {
		prefix += "_" + i.Columns[j].Name
	}
	i.Name = prefix
}

func (i *Index) IsTemporary() bool {
	return i.temporary
}

func (i *Index) SetTemporaryFlag(flag bool) {
	i.temporary = flag
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

			if j < colCount-1 {
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
			if j < colCount-1 {
				value = &Value{
					Key: columnsKeys[column],
					Tree: &BTree{
						Degree: i.tree.Degree,
					},
				}
			} else {
				value = &Value{
					Key:  columnsKeys[column],
					Data: positions,
				}
			}
			tree.Insert(value)
		}
	}
}

func (i *Index) Traverse(orderColumns map[string]string, whereCallback func(column string, value key.BytesKey) bool, callback func(positions []int) bool) bool {

	var traverse func(tree *BTree, depth int) bool
	var direction string
	var ok bool

	traverse = func(tree *BTree, depth int) bool {
		if tree.Root == nil {
			return true
		}

		column := i.Columns[depth].Name
		if direction, ok = orderColumns[column]; !ok {
			direction = "ASC"
		}

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

func (i *Index) GetColumnsForIndex(columns config.ColumnsConfig, cond types.CompareConditions, order query.Order) config.ColumnsConfig {
	indexColumns := config.ColumnsConfig{}
	indexColumnsMap := columns.GetMap()
	indexColumnsAdded := map[string]bool{}

	for i := range order {
		column := order[i].Column
		if _, ok := indexColumnsMap[column]; ok {
			if _, ok := indexColumnsAdded[column]; !ok {
				indexColumns = append(indexColumns, indexColumnsMap[column])
				indexColumnsAdded[column] = true
			}
		} else {
			panic("Column " + column + " not found")
		}
	}

	for i := range cond {
		column := cond[i].Field
		if _, ok := indexColumnsMap[column]; ok {
			if _, ok := indexColumnsAdded[column]; !ok {
				indexColumns = append(indexColumns, indexColumnsMap[column])
				indexColumnsAdded[column] = true
			}
		} else {
			panic("Column " + column + " not found")
		}
	}

	return indexColumns
}

func (i *Index) BuildIndex(storage storage.Storage) {
	rowsCount := storage.GetRowsCount()
	row := map[string]key.BytesKey{}

	for position := 0; position < rowsCount; position++ {
		for _, column := range i.Columns {
			row[column.Name] = storage.GetBytes(position, column.Name)
		}
		i.Add(position, row)
	}
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
	blockSize := 100 * 1024 * 1024

	colCount := len(i.Columns)
	bytes := make([]byte, blockSize)
	row := map[string]key.BytesKey{}

	readMore := func(position, need, bytesCount int) (error, int, int) {
		for position+need > bytesCount {
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
		return nil, position, bytesCount
	}

	columnLengths := 5 // length for positions count int32

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
			p += 5

			//fmt.Println("key", bytes[p : p+l])
			row[i.Columns[j].Name] = key.BytesKey(bytes[p : p+l])
			p += l
		}

		//fmt.Println("length", bytes[p : p+5])
		l := int(smconv.Int32FromBytes(bytes[p : p+5]))
		p += 5

		err, p, bytesCount = readMore(p, l*5, bytesCount)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		positions := make([]int, l)
		for k := 0; k < l; k++ {
			positions[k] = int(smconv.Int32FromBytes(bytes[p : p+5]))
			p += 5
		}
		//fmt.Println("load positions:", positions)
		i.Set(positions, row)
	}

	fmt.Println("load finished:", time.Now().Sub(st))

	return err
}

func (i *Index) Save() error {
	if i.IsTemporary() {
		fmt.Println("Don't save temporary index", i.GetName())
		return nil
	}

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
			if value.Tree != nil && value.Tree.Root != nil {
				depth++
				value.Tree.Root.InfixTraverseItems(fn)
				depth--
			} else if value.Data != nil {
				// save keys
				for j := 0; j < len(keys); j++ {
					w.Write(smconv.Int32ToBytes(int32(len(keys[j]))))
					w.Write(keys[j])
				}
				// save positions
				w.Write(smconv.Int32ToBytes(int32(len(value.Data))))
				//fmt.Println("save positions:", value.Data)
				for j := 0; j < len(value.Data); j++ {
					w.Write(smconv.Int32ToBytes(int32(value.Data[j])))
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
