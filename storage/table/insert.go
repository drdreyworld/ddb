package table

import (
	"ddb/types/query"
	"github.com/drdreyworld/smconv"
)

func (t *Table) Insert(ins *query.Insert) (err error) {

	for i := 0; i < len(ins.Values); i++ {
		row := map[string][]byte{}

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
			t.indexes[i].Insert(rowid, row)
		}

		t.storage.SetRowBytesByIndex(rowid, row)
	}
	return nil
}