package driver

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"reflect"
	"time"
)

type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	MaxId   int      `json:"max_id"`
}

func OpenTable(name string) (*Table, error) {
	t := &Table{}
	t.Name = name

	err := t.Load()
	return t, err
}

func CreateTable(name string, columns []Column) (*Table, error) {
	t := &Table{}
	t.Name = name
	t.Columns = columns

	for i := range t.Columns {
		t.Columns[i].Init(t)
	}

	filename := t.GetFileName()
	_, err := os.Stat(filename)
	if err == nil {
		return nil, errors.New("table already exists")
	} else if os.IsNotExist(err) {
		// ok, file not exists, lets try to create
	} else {
		return nil, err
	}

	return t, nil
}

func (t *Table) Insert(row interface{}) {
	rvalue := reflect.ValueOf(row)
	rtype := reflect.TypeOf(row)
	rowid := t.MaxId

	t.MaxId++

	for i := range t.Columns {
		col := &t.Columns[i]

		// @todo required attribute in column
		if value, ok := rtype.FieldByName(col.Name); !ok {
			log.Fatalln("Can't get row column by name '", col.Name, "' in row ", row)
		} else {
			if value.Type.Name() == col.Type {
				col.SetValue(rowid, rvalue.FieldByName(col.Name).Interface())
			} else {
				log.Fatalln("Invalid field type for column", col.Name, ": ", value.Type.Name())
			}
		}
	}
}

func (t *Table) GetById(id int, row interface{}) {
	for i := range t.Columns {
		col := &t.Columns[i]
		if b, ok := col.GetValue(id); ok {
			ValueFromBytes(b, reflect.ValueOf(row).Elem().FieldByName(col.Name))
		} else {
			log.Fatalln("can't get value by id:", id, "in column", col.Name)
		}
	}
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

func (t *Table) saveTableInfo() {

	f, err := os.OpenFile(t.GetFileName(), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		panic(err)
		log.Fatalln(err)
	}
	defer f.Close()

	if b, err := json.Marshal(t); err != nil {
		log.Fatal(err)
	} else {
		f.Write(b)
	}

	if err := f.Sync(); err != nil {
		panic(err)
		log.Fatal(err)
	}
}

func (t *Table) saveColumns() {
	for i := range t.Columns {
		t.Columns[i].Save()
	}
}

func (t *Table) Save() {
	t.saveColumns()
	t.saveTableInfo()
}

func (t *Table) loadTableInfo() (err error) {
	f, err := os.OpenFile(t.GetFileName(), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	data := []byte{}

	for r := 0; ; r++ {
		b := make([]byte, 1024)
		if n, err := f.Read(b); err != nil {
			if err == io.EOF {
				break
			}
			return err
		} else {
			data = append(data, b[:n]...)
		}
	}

	return json.Unmarshal(data, t)
}

func (t *Table) loadColumns() error {
	for i := range t.Columns {
		t.Columns[i].Init(t)
		if err := t.Columns[i].Load(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Load() error {
	if err := t.loadTableInfo(); err != nil {
		return err
	}

	if err := t.loadColumns(); err != nil {
		return err
	}
	return nil
}

func (t *Table) ColumnByName(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

type FindFieldCond struct {
	Field string
	Value interface{}
	Bytes []byte
}

func (t *Table) Find(field string, value interface{}, limit int) *[]int {
	return t.FindByCond([]FindFieldCond{{Field: field, Value: value}}, limit)
}

func (t *Table) FindByCond(cond []FindFieldCond, limit int) *[]int {
	res := []int{}
	cols := map[string]*Column{}

	for i := range cond {
		if col := t.ColumnByName(cond[i].Field); col == nil {
			panic("column not found by name " + cond[i].Field)
		} else {
			cols[col.Name] = col

			b, err := ValueToBytes(cond[i].Value, col.Length)
			if err != nil {
				panic(err)
			}

			cond[i].Bytes = b
		}
	}

	i := 0
	s := time.Now()
	for i = 0; i < t.MaxId; i++ {
		eq := true
		for j := 0; j < len(cond) && eq; j++ {
			col := cols[cond[j].Field]

			for k := len(cond[j].Bytes)-1; k > 0 && eq; k-- {
				eq = cond[j].Bytes[k] == col.values[i][k]
			}
		}

		if eq {
			res = append(res, i)
		}

		if limit > 0 && len(res) >= limit {
			break
		}
	}
	log.Println("scan rows", i)
	log.Println("time", time.Now().Sub(s))

	return &res
}

func (t *Table) CountByCond(cond []FindFieldCond) int {
	res := 0
	cols := map[string]*Column{}

	for i := range cond {
		if col := t.ColumnByName(cond[i].Field); col == nil {
			panic("column not found by name " + cond[i].Field)
		} else {
			cols[col.Name] = col

			b, err := ValueToBytes(cond[i].Value, col.Length)
			if err != nil {
				panic(err)
			}

			cond[i].Bytes = b
		}
	}

	i := 0
	s := time.Now()
	for i = 0; i < t.MaxId; i++ {
		eq := true
		for j := 0; j < len(cond) && eq; j++ {
			col := cols[cond[j].Field]

			for k := len(cond[j].Bytes) - 1; k > 0 && eq; k-- {
				eq = cond[j].Bytes[k] == col.values[i][k]
			}
		}

		if eq {
			res++
		}
	}
	log.Println("scan rows", i)
	log.Println("time", time.Now().Sub(s))

	return res
}
