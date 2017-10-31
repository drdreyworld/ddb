package table

import (
	"ddb/types/key"
	"reflect"
	"errors"
	"ddb/types/query"
	"github.com/drdreyworld/smconv"
)

func (t *Table) Insert(ins *query.Insert) (err error) {

	for i := 0; i < len(ins.Values); i++ {
		row := map[string]key.BytesKey{}

		for j := 0; j < len(ins.Columns); j++ {
			col := string(ins.Columns[j])
			cfg := t.config.Columns.ByName(col)

			row[col] = smconv.StringValueToBytes(
				string(ins.Values[i][j].Data),
				cfg.Type,
				cfg.Length,
			)
		}

		rowid := t.storage.GetRowsCount()

		for i := range t.indexes {
			t.indexes[i].Add(rowid, row)
		}

		for _, col := range t.config.Columns {
			t.storage.SetBytes(rowid, col.Name, row[col.Name])
		}
	}
	return nil
}

func (t *Table) InsertOld(data interface{}, addToIndex bool) (err error) {
	row := make(map[string]key.BytesKey)

	rvalue := reflect.ValueOf(data)
	rtype := reflect.TypeOf(data)

	for _, col := range t.config.Columns {

		if value, ok := rtype.FieldByName(col.Name); !ok {
			return errors.New("Can't get row column by name '" + col.Name + "' in row ")
		} else {
			if value.Type.Name() == col.Type {
				row[col.Name] = smconv.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
			} else {
				return errors.New("Invalid field type for column '" + col.Name + "': '" + value.Type.Name() + "'")
			}
		}
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
