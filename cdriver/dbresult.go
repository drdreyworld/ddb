package cdriver

import (
	"reflect"
	"errors"
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

func (r *DbResult) SetPositions(p map[int]int) {
	r.positions = make([]int, len(p))
	r.current = 0
	for position := range p {
		r.positions[r.current] = position
		r.current++
	}
	r.current = 0
}

func (r *DbResult) GetRowsCount() int {
	return len(r.positions)
}

func (r *DbResult) Rewind() {
	r.current = 0
}

func (r *DbResult) FetchRow(row interface{}) error {
	if r.current >= len(r.positions) {
		return nil
	}

	res := r.table.Columns.GetRowByIndex(r.positions[r.current])
	ref := reflect.ValueOf(row).Elem()

	for name, val := range res {
		err := ValueFromBytes(val, ref.FieldByName(name))

		if err != nil {
			return errors.New("banana")
			//return err
		}
	}

	r.current++

	return nil
}
