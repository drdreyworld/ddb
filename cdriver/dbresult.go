package cdriver

import (
	"reflect"
)

type DbResult struct {
	table     *Table
	positions map[int]int
}

func (r *DbResult) Init(t *Table) {
	r.table = t
	r.positions = map[int]int{}
}

func (r *DbResult) SetPositions(p map[int]int) {
	r.positions = p
}

func (r *DbResult) GetRowsCount() int {
	return len(r.positions)
}

func (r *DbResult) FetchRow(row interface{}) error {

	for pos := range r.positions {
		for i := 0; i < len(r.table.Columns); i++ {
			col := &r.table.Columns[i]
			err := ValueFromBytes(col.GetBytes(pos), reflect.ValueOf(row).Elem().FieldByName(col.Name))

			if err != nil {
				return err
			}
		}

		break
	}

	return nil
}
