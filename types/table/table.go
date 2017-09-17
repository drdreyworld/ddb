package table

import (
	"ddb/types/funcs"
	"ddb/types"
	"ddb/types/config"
	"ddb/types/index"
	"ddb/types/index/mbbtree"
	"ddb/types/storage"
	"ddb/types/storage/colstor"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
	"ddb/types/query"
	"ddb/types/dbresult"
)

type Table struct {
	name   string
	config config.TableConfig

	storage storage.Storage
	indexes index.Indexes
}

func CreateIndex(indexType string) index.Index {
	switch indexType {
	case "mbbtree":
		return &mbbtree.Index{}
	default:
		panic("Unknown index type")
	}
}

func CreateStorage(storageType string) storage.Storage {
	switch storageType {
	case "colstor":
		return &colstor.Columns{}
	default:
		panic("Unknown index type")
	}
}

func OpenTable(name string) (t *Table, err error) {
	t = &Table{
		name: name,
	}

	if err = t.loadTableInfo(); err != nil {
		return nil, err
	}

	t.initIndexes()

	t.storage = CreateStorage(t.config.Storage)
	t.storage.Init(t.name, t.config.Columns)
	t.storage.Load()

	t.buildIndexes()

	return t, err
}

func CreateTable(name string, config config.TableConfig) (*Table, error) {
	t := &Table{}
	t.name = name

	t.storage = CreateStorage(t.config.Storage)
	t.storage.Init(t.name, t.config.Columns)

	if res, err := t.isTableFileExists(); err != nil {
		return nil, err
	} else if res {
		return nil, errors.New("table already exists")
	}
	return t, nil
}

func (t *Table) Save() {
	if err := t.saveTableInfo(); err != nil {
		panic(err)
	}

	if err := t.storage.Save(); err != nil {
		panic(err)
	}

	t.indexes.Save()
}

func (t *Table) GetStorage() storage.Storage {
	return t.storage
}

func (t *Table) initIndexes() {
	for _, i := range t.config.Indexes {
		idx := CreateIndex(i.Type)
		idx.Init(i.Name, t.name)
		idx.SetColumns(i.Cols)

		t.indexes = append(t.indexes, idx)
	}
}

func (t *Table) buildIndexes() {
	for i := 0; i < len(t.indexes); i++ {
		if err := t.indexes[i].Load(); err != nil {
			t.indexes[i].BuildIndex(t.storage, types.CompareConditions{}, query.Order{})
			t.indexes[i].Save()
		}
	}
}

func (t *Table) ReBuildIndexes() {
	t.indexes = index.Indexes{}

	for _, i := range t.config.Indexes {
		idx := CreateIndex(i.Type)
		idx.Init(i.Name, t.name)
		idx.SetColumns(i.Cols)
		idx.BuildIndex(t.storage, types.CompareConditions{}, query.Order{})
		t.indexes = append(t.indexes, idx)
	}
}

func (t *Table) PrepareRow(row interface{}) (map[string]interface{}, error) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)

	result := map[string]interface{}{}

	for _, col := range t.config.Columns {

		if value, ok := rtype.FieldByName(col.Name); !ok {
			return nil, errors.New("Can't get row column by name '" + col.Name + "' in row ")
		} else {
			if value.Type.Name() == col.Type {
				val, err := funcs.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
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

func (t *Table) Insert(data interface{}, addToIndex bool) (err error) {
	var row map[string]interface{}

	if row, err = t.PrepareRow(data); err != nil {
		return err
	}

	rowid := t.storage.GetRowsCount()

	if addToIndex {
		for i := range t.indexes {
			t.indexes[i].Add(rowid, row)
		}
	}

	for _, col := range t.config.Columns {
		t.storage.SetBytes(rowid, col.Name, row[col.Name].([]byte))
	}

	return nil
}

func (t *Table) Update(id int, row interface{}) (err error) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)

	if id >= t.storage.GetRowsCount() {
		return errors.New("ID out of range")
	}

	for _, col := range t.config.Columns {

		if value, ok := rtype.FieldByName(col.Name); !ok {
			log.Fatalln("Can't get row column by name '", col.Name, "' in row ", row)
		} else {
			if value.Type.Name() == col.Type {
				b, err := funcs.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
				if err != nil {
					panic("Can't convert value to bytes")
				}
				t.storage.SetBytes(id, col.Name, b)
			} else {
				log.Fatalln("Invalid field type for column", col.Name, ": ", value.Type.Name())
			}
		}
	}

	return nil
}

func (t *Table) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + t.name
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

	if data, err = json.Marshal(t.config); err != nil {
		return err
	}

	return ioutil.WriteFile(t.GetFileName(), data, 0777)
}

func (t *Table) loadTableInfo() (err error) {
	var data []byte

	if data, err = ioutil.ReadFile(t.GetFileName()); err != nil {
		return err
	}

	return json.Unmarshal(data, &t.config)
}

func (t *Table) convertCondToRow(cond []types.CompareCondition) (row map[string][]byte) {
	cols := t.config.Columns
	colsMap := map[string]int{}
	for _, c := range cols {
		colsMap[c.Name] = c.Length
	}

	row = map[string][]byte{}

	for c := range cond {
		if colLength, ok := colsMap[cond[c].Field]; !ok {
			panic("column not found by name " + cond[c].Field)
		} else {
			b, err := funcs.ValueToBytes(cond[c].Value, colLength)
			if err != nil {
				panic(err)
			}

			row[cond[c].Field] = b
		}
	}
	return row
}

func (t *Table) CreateFindCond(where query.Where) (res []types.CompareCondition, err error) {
	res = []types.CompareCondition{}
	for i := range where {
		c := t.config.Columns.ByName(where[i].OperandA)
		if c == nil {
			return nil, errors.New("Unknown column " + where[i].OperandA)
		}

		switch c.Type {

		case "int32":
			val, err := strconv.Atoi(where[i].OperandB)
			if err != nil {
				return nil, err
			}

			ival, err := funcs.ValueToBytes(val, c.Length)
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
			sval, err := funcs.ValueToBytes(where[i].OperandB, c.Length)
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

func (t *Table) GetIndex(cond types.CompareConditions, order query.Order) index.Index {
	columnsMap := map[string]bool{}

	for i := range cond {
		columnsMap[cond[i].Field] = true
	}

	for i := range order {
		columnsMap[order[i].Column] = true
	}

	for _, idx := range t.indexes {
		if ok := len(columnsMap) == len(idx.GetColumns()); ok {
			for _, column := range idx.GetColumns() {
				if _, ok = columnsMap[column]; !ok {
					break
				}
			}

			if ok {
				return idx
			}
		}
	}
	return nil
}

func (t *Table) Select(cols query.SelectExprs, where query.Where, order query.Order, limit query.Limit) (res *dbresult.DbResult, err error) {
	limit.PrepareLimit(t.storage.GetRowsCount())

	var cond types.CompareConditions
	var whereCallback index.WhereCallback

	if cond, err = t.CreateFindCond(where); err != nil {
		return nil, err
	}

	ordersColumns := map[string]string{}

	for i := range order {
		ordersColumns[order[i].Column] = order[i].Direction
	}

	var idx index.Index

	if idx = t.GetIndex(cond, order); idx == nil {
		if len(ordersColumns) == 0 {
			fmt.Println("Search without index")
			return t.SearchWithoutIndex(cols, cond, limit)
		} else {
			fmt.Println("Build index")
			idx = CreateIndex(t.config.IndexType)
			idx.BuildIndex(t.storage, cond, order)

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

	res = &dbresult.DbResult{}
	res.Init(t.storage)
	res.SetPositions(positions)
	return res, nil
}

func (t *Table) SearchWithoutIndex(cols query.SelectExprs, cond types.CompareConditions, limit query.Limit) (res *dbresult.DbResult, err error) {

	conds := map[string]types.CompareConditions{}

	for _, column := range t.storage.GetColumns() {
		conds[column] = cond.ByColumnName(column)
	}

	positions := []int{}
	j := 0

	for i := 0; i < t.storage.GetRowsCount(); i++ {
		matched := true

		for ci := range t.config.Columns {
			column := &t.config.Columns[ci]
			for _, columnCond := range conds[column.Name] {
				if matched = columnCond.Compare(t.storage.GetBytes(i, column.Name)); !matched {
					break
				}
			}
			if !matched {
				break
			}
		}

		if matched {
			if j >= limit.Offset {
				positions = append(positions, i)
			}
			j++
			if j >= limit.RowCount {
				break
			}
		}
	}

	res = &dbresult.DbResult{}
	res.Init(t.storage)
	res.SetPositions(positions)
	return res, nil
}
