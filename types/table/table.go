package table

import (
	"ddb/types/config"
	"ddb/types/index"
	"ddb/types/index/btree"
	"ddb/types/storage"
	"ddb/types/storage/colstor"
	"ddb/types/storage/bptree"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"fmt"
)

type Table struct {
	name   string
	config config.TableConfig

	storage storage.Storage
	indexes index.Indexes

	Lock bool
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
	case "bptree":
		return &bptree.Storage{}
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

func (t *Table) Close() {
	t.storage.Close()
	// t.indexes.Save()
}
func (t *Table) Save() {
	if err := t.saveTableInfo(); err != nil {
		panic(err)
	}

	if err := t.storage.Flush(); err != nil {
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
		idx.Init()

		idx.SetName(i.Name)
		idx.SetTableName(t.name)
		idx.SetColumns(i.Cols)

		t.indexes = append(t.indexes, idx)
	}
}

func (t *Table) buildIndexes() {
	for i := 0; i < len(t.indexes); i++ {
		if err := t.indexes[i].Load(); err != nil {
			fmt.Println(err)
			if err.Error() == "EOF" {
				fmt.Println("ignore error")
				continue
			}

			t.indexes[i].BuildIndex(t.storage)
			t.indexes[i].Save()
		}
	}
}

func (t *Table) ReBuildIndexes() {
	t.indexes = index.Indexes{}

	for _, i := range t.config.Indexes {
		idx := CreateIndex(i.Type)
		idx.Init()
		idx.SetName(i.Name)
		idx.SetTableName(t.name)
		idx.SetColumns(i.Cols)
		idx.BuildIndex(t.storage)
		t.indexes = append(t.indexes, idx)
	}
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