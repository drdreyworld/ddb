package table

import (
	"ddb/types/funcs"
	"ddb/types"
	"ddb/types/config"
	"ddb/types/index"
	"ddb/types/index/btree"
	"ddb/types/storage"
	"ddb/types/storage/colstor"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"ddb/types/query"
	"ddb/types/key"
)

type Table struct {
	name   string
	config config.TableConfig

	storage storage.Storage
	indexes index.Indexes
}

func CreateIndex(indexType string) index.Index {
	switch indexType {
	case "btree":
		return &btree.Index{}
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

func (t *Table) PrepareRow(row interface{}) (result map[string]key.BytesKey, err error) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)

	for _, col := range t.config.Columns {

		if value, ok := rtype.FieldByName(col.Name); !ok {
			return nil, errors.New("Can't get row column by name '" + col.Name + "' in row ")
		} else {
			if value.Type.Name() == col.Type {
				result[col.Name], err = funcs.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, errors.New("Invalid field type for column '" + col.Name + "': '" + value.Type.Name() + "'")
			}
		}
	}
	return result, nil
}

func (t *Table) Insert(data interface{}, addToIndex bool) (err error) {
	var row map[string]key.BytesKey

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
		t.storage.SetBytes(rowid, col.Name, row[col.Name])
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
