package table

import (
	"ddb/types/funcs"
	"ddb/types/key"
	"reflect"
	"errors"
	"ddb/types/query"
	"strconv"
)

func (t *Table) Insert(ins *query.Insert) (err error) {

	for i := 0; i < len(ins.Values); i++ {
		row := map[string]key.BytesKey{}

		for j := 0; j < len(ins.Columns); j++ {
			col := string(ins.Columns[j])

			switch t.config.Columns.ByName(col).Type {
			case "int32":
				s := string(ins.Values[i][j].Data)

				i, err := strconv.Atoi(s)
				if err != nil {
					return err
				}

				row[col], err = funcs.ValueToBytes(i, t.config.Columns.ByName(col).Length)
				if err != nil {
					return err
				}

				break;

			case "string":
				row[col], err = funcs.ValueToBytes(ins.Values[i][j].Data, t.config.Columns.ByName(col).Length)
				if err != nil {
					return err
				}

				break;

			default:
				panic("unknown column type")
			}
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
				row[col.Name], err = funcs.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
				if err != nil {
					return err
				}
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
