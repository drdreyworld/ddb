package cdriver

import (
	"errors"
	"reflect"
	"strconv"
	"fmt"
)

type DbResult struct {
	table     *Table
	current   int
	positions []int
}

func (r *DbResult) Init(t *Table) {
	r.table = t
	r.positions = []int{}
}

func (r *DbResult) SetPositions(p []int) {
	r.current = 0
	r.positions = p
}

func (r *DbResult) GetRowsCount() int {
	return len(r.positions)
}

func (r *DbResult) Rewind() {
	r.current = 0
}

func (r *DbResult) FetchStruct(row interface{}) error {
	if r.current >= len(r.positions) {
		return errors.New("EOF")
	}

	res := r.table.Columns.GetRowByIndex(r.positions[r.current])
	ref := reflect.ValueOf(row).Elem()

	for name, val := range res {
		err := ValueFromBytes(val, ref.FieldByName(name))

		if err != nil {
			return err
		}
	}

	r.current++

	return nil
}

func (r *DbResult) FetchMap() (map[string]string, error) {
	if r.current >= len(r.positions) {
		return nil, errors.New("EOF")
	}

	res := r.table.Columns.GetRowByIndex(r.positions[r.current])
	row := map[string]string{}

	for name, value := range res {

		switch r.table.Columns.ByName(name).Type {

		case "int64":
			if val, err := DecodeValueInt(value); err != nil {
				return nil, err
			} else {
				row[name] = strconv.Itoa(val)
			}
			break

		case "string":
			if val, err := DecodeValueStr(value); err != nil {
				return nil, err
			} else {
				row[name] = val
			}
			break
		default:
			return nil, errors.New(fmt.Sprintf("unsupported column type '%s'", r.table.Columns.ByName(name).Type))
		}
	}

	r.current++

	return row, nil
}
