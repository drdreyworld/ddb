package cdriver

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

func (r *DbResult) FetchRow() map[string]interface{} {
	row := map[string]interface{}{}

	for pos := range r.positions {
		for i := 0; i < len(r.table.Columns); i++ {
			col := &r.table.Columns[i]
			row[col.Name], _ = col.GetBytes(pos)
		}
		break
	}

	return row
}
