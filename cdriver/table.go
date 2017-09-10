package cdriver

import (
	"ddb/structs/types"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
)

type Table struct {
	Name    string  `json:"name"`
	Columns Columns `json:"columns"`
	MaxId   int     `json:"max_id"`
	Indexes []Index `json:"indexes"`
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

	t.buildIndexes()

	return t, err
}

func CreateTable(name string, columns []Column) (*Table, error) {
	t := &Table{}
	t.Name = name
	t.Columns = columns

	t.initIndexes()
	t.Columns.Init(t)

	if res, err := t.isTableFileExists(); err != nil {
		return nil, err
	} else if res {
		return nil, errors.New("table already exists")
	}
	return t, nil
}

func (t *Table) initIndexes() {
	for i := range t.Indexes {
		t.Indexes[i].Init(t)
	}
}

func (t *Table) buildIndexes() {
	for i := 0; i < t.Columns.GetRowsCount(); i++ {
		row := t.Columns.GetRowByIndex(i)
		for j := range t.Indexes {
			idx := &t.Indexes[j]
			idx.Add(i, row)
		}
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

	for i := range t.Indexes {
		idx := &t.Indexes[i]
		idx.Add(rowid, row)
	}

	for i := range t.Columns {
		col := &t.Columns[i]
		// @todo required attribute in column
		col.SetBytes(rowid, row[col.Name])
	}
	return nil
}

func (t *Table) GetByIndex(index int, row interface{}) error {
	for i := range t.Columns {
		col := &t.Columns[i]
		cel := col.GetBytes(index)
		err := ValueFromBytes(cel, reflect.ValueOf(row).Elem().FieldByName(col.Name))
		if err != nil {
			return err
		}
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

type FindFieldCond struct {
	Field      string
	Value      interface{}
	Compartion string
}

func (t *Table) GetBestIndexByCond(cond []FindFieldCond) (idx *Index) {
	// количество столбцов в индексе
	idxLen := len(cond)

	for i := range t.Indexes {
		idx = &t.Indexes[i]
		if len(idx.Columns) != idxLen {
			idx = nil
			continue
		}

		for c := range cond {
			if idx.Columns[c] != cond[c].Field {
				idx = nil
				break
			}
		}

		if idx != nil {
			break
		}
	}

	return idx
}

func (t *Table) GetAnyIndexByCond(cond []FindFieldCond) (idx *Index) {
	for i := range t.Indexes {
		idx = &t.Indexes[i]

		for c := range cond {
			if len(idx.Columns) <= c || idx.Columns[c] != cond[c].Field {
				idx = nil
				break
			}
		}

		if idx != nil {
			break
		}
	}

	return idx
}

func (t *Table) GetIndexByCond(cond []FindFieldCond) (idx *Index) {
	if idx = t.GetBestIndexByCond(cond); idx != nil {
		return idx
	}
	return t.GetAnyIndexByCond(cond)
}

func (t *Table) convertCondToRow(cond []FindFieldCond) (row map[string][]byte) {
	cols := map[string]*Column{}
	row = map[string][]byte{}

	for c := range cond {
		if col := t.Columns.ByName(cond[c].Field); col == nil {
			panic("column not found by name " + cond[c].Field)
		} else {
			cols[col.Name] = col

			b, err := ValueToBytes(cond[c].Value, col.Length)
			if err != nil {
				panic(err)
			}

			row[cond[c].Field] = b
		}
	}
	return row
}

func (t *Table) FindByIndex(cond []FindFieldCond, limit, offset int) (res *DbResult, err error) {
	var idx *Index

	if idx = t.GetIndexByCond(cond); idx == nil {
		return nil, errors.New(fmt.Sprintf("Index not matched by cond %v", cond))
	}

	return idx.Find(t.convertCondToRow(cond), limit, offset), nil
}

func (t *Table) CreateFindCond(where types.Where) (res []FindFieldCond, err error) {
	res = []FindFieldCond{}
	for i := range where {
		c := t.Columns.ByName(where[i].OperandA)
		if c == nil {
			return nil, errors.New("Unknown column " + where[i].OperandA)
		}

		switch c.Type {

		case "int64":
			val, err := strconv.Atoi(where[i].OperandB)
			if err != nil {
				return nil, err
			}
			res = append(res, FindFieldCond{
				Field:      where[i].OperandA,
				Value:      val,
				Compartion: where[i].Compartion,
			})
			break

		case "string":
			res = append(res, FindFieldCond{
				Field:      where[i].OperandA,
				Value:      where[i].OperandB,
				Compartion: where[i].Compartion,
			})

			break
		default:
			return nil, errors.New("Unknown column type " + c.Type)
		}
	}
	return res, nil
}

func (t *Table) Select(cols types.Columns, where types.Where, limit, offset int) (res *DbResult, err error) {
	var idx *Index
	var cond []FindFieldCond

	if cond, err = t.CreateFindCond(where); err != nil {
		return nil, err
	}

	if len(cond) > 0 {
		if idx = t.GetIndexByCond(cond); idx != nil {
			return idx.Find(t.convertCondToRow(cond), limit, offset), nil
		}
		//  @TODO FIND By columns
	}

	res = &DbResult{}
	res.Init(t)

	if limit == 0 && offset == 0 {
		limit = t.Columns.GetRowsCount()
	}

	if offset >= t.Columns.GetRowsCount() {
		offset = t.Columns.GetRowsCount() - 1
		limit = 0
	}

	if limit > t.Columns.GetRowsCount()-offset {
		limit = t.Columns.GetRowsCount() - offset
	}

	pos := make([]int, limit)
	for i := 0; i < limit; i++ {
		pos[i] = offset + i
	}
	res.SetPositions(pos)

	return res, nil
}
