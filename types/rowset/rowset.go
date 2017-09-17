package rowset

type Rowset struct {
	Cols []Col
	Rows []Row
}

type Col struct {
	Name   string
	Length int
	Type   byte
}

type Row []string
