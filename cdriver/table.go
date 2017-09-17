package cdriver

import (
	"ddb/structs/mbbtree"
	"ddb/structs/types"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
)

type Table struct {
	Name    string  `json:"name"`
	Columns Columns `json:"columns"`
	MaxId   int     `json:"max_id"`

	IndexesConf []types.IndexConfig `json:"indexes"`
	Indexes     []types.Index

	IndexType string
	Index     types.Index
}

func OpenTable(name string) (t *Table, err error) {
	t = &Table{}
	t.Name = name

	if err = t.loadTableInfo(); err != nil {
		return nil, err
	}

	t.initIndexes()
	t.Columns.Init(t)

	if err = t.Columns.Load(); err != nil {
		return nil, err
	}

	t.IndexType = "mbbtree"

	switch t.IndexType {
	case "mbbtree":
		t.Index = &mbbtree.Index{}
		break
	default:
		panic("Unknown index type" + t.IndexType)
	}

	t.buildIndexes()

	return t, err
}

func CreateTable(name string, columns []Column) (*Table, error) {
	t := &Table{}
	t.Name = name
	t.Columns = columns

	t.Columns.Init(t)

	if res, err := t.isTableFileExists(); err != nil {
		return nil, err
	} else if res {
		return nil, errors.New("table already exists")
	}
	return t, nil
}

func (t *Table) initIndexes() {
	for _, i := range t.IndexesConf {
		var index types.Index
		switch i.Type {
		case "mbbtree":
			index = &mbbtree.Index{}
			break
		default:
			panic("Unknown index type" + i.Type)
			break
		}

		index.SetColumns(i.Columns)

		t.Indexes = append(t.Indexes, index)
	}
}

func (t *Table) buildIndexes() {
	for i := 0; i < len(t.Indexes); i++ {
		t.Indexes[i] = t.Indexes[i].BuildIndex(&t.Columns, types.CompareConditions{}, types.Order{})
	}
}

func (t *Table) PrepareRow(row interface{}) (map[string][]byte, error) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)

	result := make(map[string][]byte)

	for i := range t.Columns {
		col := &t.Columns[i]

		if value, ok := rtype.FieldByName(col.Name); !ok {
			return nil, errors.New("Can't get row column by name '" + col.Name + "' in row ")
		} else {
			if value.Type.Name() == col.Type {
				val, err := ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
				if err != nil {
					return nil, err
				}
				result[col.Name] = val
			} else {
				return nil, errors.New("Invalid field type for column '" + col.Name + "': '" + value.Type.Name() + "'")
			}
		}
	}
	return result, nil
}

func (t *Table) Insert(data interface{}) (err error) {
	var row map[string][]byte

	if row, err = t.PrepareRow(data); err != nil {
		return err
	}

	rowid := t.MaxId
	t.MaxId++

	//for i := range t.Indexes {
	//idx := &t.Indexes[i]
	//idx.Add(rowid, row)
	//}

	for i := range t.Columns {
		col := &t.Columns[i]
		// @todo required attribute in column
		col.SetBytes(rowid, row[col.Name])
	}
	return nil
}

func (t *Table) Update(id int, row interface{}) (err error) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)

	if id >= t.MaxId {
		return errors.New("ID out of range")
	}

	for i := range t.Columns {
		col := &t.Columns[i]

		if value, ok := rtype.FieldByName(col.Name); !ok {
			log.Fatalln("Can't get row column by name '", col.Name, "' in row ", row)
		} else {
			if value.Type.Name() == col.Type {
				col.SetValue(id, rvalue.FieldByName(col.Name).Interface())
			} else {
				log.Fatalln("Invalid field type for column", col.Name, ": ", value.Type.Name())
			}
		}
	}

	return nil
}

func (t *Table) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + t.Name
}

func (t *Table) isTableFileExists() (res bool, err error) {
	filename := t.GetFileName()
	_, err = os.Stat(filename)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (t *Table) saveTableInfo() (err error) {
	var data []byte

	if data, err = json.Marshal(t); err != nil {
		return err
	}

	return ioutil.WriteFile(t.GetFileName(), data, 0777)
}

func (t *Table) loadTableInfo() (err error) {
	var data []byte

	if data, err = ioutil.ReadFile(t.GetFileName()); err != nil {
		return err
	}

	return json.Unmarshal(data, t)
}

func (t *Table) convertCondToRow(cond []types.CompareCondition) (row map[string][]byte) {
	row = map[string][]byte{}

	for c := range cond {
		if col := t.Columns.ByName(cond[c].Field); col == nil {
			panic("column not found by name " + cond[c].Field)
		} else {
			b, err := ValueToBytes(cond[c].Value, col.Length)
			if err != nil {
				panic(err)
			}

			row[cond[c].Field] = b
		}
	}
	return row
}

func (t *Table) CreateFindCond(where types.Where) (res []types.CompareCondition, err error) {
	res = []types.CompareCondition{}
	for i := range where {
		c := t.Columns.ByName(where[i].OperandA)
		if c == nil {
			return nil, errors.New("Unknown column " + where[i].OperandA)
		}

		switch c.Type {

		case "int32":
			val, err := strconv.Atoi(where[i].OperandB)
			if err != nil {
				return nil, err
			}

			ival, err := ValueToBytes(val, c.Length)
			if err != nil {
				return nil, err
			}

			res = append(res, types.CompareCondition{
				Field:      where[i].OperandA,
				Value:      ival,
				Compartion: where[i].Compartion,
			})
			break

		case "string":
			sval, err := ValueToBytes(where[i].OperandB, c.Length)
			if err != nil {
				return nil, err
			}

			res = append(res, types.CompareCondition{
				Field:      where[i].OperandA,
				Value:      sval,
				Compartion: where[i].Compartion,
			})

			break
		default:
			return nil, errors.New("Unknown column type " + c.Type)
		}
	}
	return res, nil
}

func (t *Table) GetIndex(cond types.CompareConditions, order types.Order) types.Index {
	columnsMap := map[string]bool{}

	for i := range cond {
		columnsMap[cond[i].Field] = true
	}

	for i := range order {
		columnsMap[order[i].Column] = true
	}

	for _, index := range t.Indexes {
		if ok := len(columnsMap) == len(index.GetColumns()); ok {
			for _, column := range index.GetColumns() {
				if _, ok = columnsMap[column]; !ok {
					break;
				}
			}

			if ok {
				return index
			}
		}
	}
	return nil
}

func (t *Table) Select(cols types.Columns, where types.Where, order types.Order, limit types.Limit) (res *DbResult, err error) {
	limit.PrepareLimit(t.Columns.GetRowsCount())

	var cond types.CompareConditions
	var whereCallback types.WhereCallback

	if cond, err = t.CreateFindCond(where); err != nil {
		return nil, err
	}

	ordersColumns := map[string]string{}

	for i := range order {
		ordersColumns[order[i].Column] = order[i].Direction
	}

	var idx types.Index

	if idx = t.GetIndex(cond, order); idx == nil {
		if len(ordersColumns) == 0 {
			fmt.Println("Search without index")
			return t.SearchWithoutIndex(cols, cond, limit)
		} else {
			fmt.Println("Build index")
			idx = t.Index.BuildIndex(&t.Columns, cond, order)
			whereCallback = nil
		}
	} else {
		fmt.Println("Use index")
		conds := map[string]types.CompareConditions{}
		for _, column := range idx.GetColumns() {
			conds[column] = cond.ByColumnName(column)
		}

		whereCallback = func(column string, value []byte) bool {
			result := true
			for _, cnd := range conds[column] {
				if result = result && cnd.Compare(value); !result {
					return result
				}
			}
			return result
		}
	}

	for _, columnName := range idx.GetColumns() {
		if _, ok := ordersColumns[columnName]; !ok {
			ordersColumns[columnName] = "ASC"
		}
	}

	positions := make([]int, 0, limit.RowCount)

	st := time.Now()
	fmt.Println("Traverse index: ")
	idx.Traverse(ordersColumns, whereCallback, func(pos []int) bool {
		for i := range pos {
			positions = append(positions, pos[i])

			if len(positions) >= limit.RowCount {
				return false
			}
		}

		return true
	})
	fmt.Println(" complete:", time.Now().Sub(st))

	res = &DbResult{}
	res.Init(t)
	res.SetPositions(positions)
	return res, nil
}

func (t *Table) SearchWithoutIndex(cols types.Columns, cond types.CompareConditions, limit types.Limit) (res *DbResult, err error) {

	conds := map[string]types.CompareConditions{}

	for _, column := range t.Columns.GetColumns() {
		conds[column] = cond.ByColumnName(column)
	}

	positions := []int{}
	j := 0

	for i := 0; i < t.Columns.GetRowsCount(); i++ {
		matched := true

		for ci := range t.Columns {
			column := &t.Columns[ci]
			for _, columnCond := range conds[column.Name] {
				if matched = columnCond.Compare(t.Columns.GetBytes(i, column.Name)); !matched {
					break;
				}
			}
			if !matched {
				break;
			}
		}

		if matched {
			if j >= limit.Offset {
				positions = append(positions, i)
			}
			j++
			if j >= limit.RowCount {
				break;
			}
		}
	}

	res = &DbResult{}
	res.Init(t)
	res.SetPositions(positions)
	return res, nil
}