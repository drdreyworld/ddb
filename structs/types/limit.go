package types

type Limit struct {
	Offset   int
	RowCount int
}

func (l *Limit) PrepareLimit(rowCount int) {
	if l.RowCount == 0 && l.Offset == 0 {
		l.RowCount = rowCount
	}

	if l.Offset >= rowCount {
		l.Offset = rowCount - 1
		l.RowCount = 0
	}

	if l.RowCount > rowCount-l.Offset {
		l.RowCount = rowCount - l.Offset
	}
}
