package table

import (
	"reflect"
	"errors"
	"log"
	"github.com/drdreyworld/smconv"
)

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
				b := smconv.ValueToBytes(rvalue.FieldByName(col.Name).Interface(), col.Length)
				t.storage.SetBytes(id, col.Name, b)
			} else {
				log.Fatalln("Invalid field type for column", col.Name, ": ", value.Type.Name())
			}
		}
	}

	return nil
}

