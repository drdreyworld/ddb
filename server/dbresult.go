package server

type Rowset struct {
	Cols []Col
	Rows []Row
}

type Col struct {
	Name   string
	Length int
	Type   byte
}

type Row struct {
	cells []string
}

func CreateUsersRowset() Rowset {
	result := Rowset{
		Cols: []Col{
			{
				"id",
				4,
				fieldTypeInt24,
			},
			{
				"first_name",
				255,
				fieldTypeVarChar,
			},
			{
				"last_name",
				255,
				fieldTypeVarChar,
			},
		},
		Rows: []Row{
			{[]string{"1", "Петя", "Иванов"}},
			{[]string{"2", "Стас", "Петров"}},
			{[]string{"3", "Вася", "Пупкин"}},
		},
	}

	return result
}
