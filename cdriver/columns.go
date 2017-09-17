package cdriver

type Columns []Column

func (c *Columns) Init(t *Table) {
	for i := range *c {
		(*c)[i].Init(t)
	}
}

func (c *Columns) Load() error {
	for i := range *c {
		if err := (*c)[i].Load(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Columns) Save() error {
	for i := range *c {
		if err := (*c)[i].Save(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Columns) ByName(name string) *Column {
	for i := range *c {
		if (*c)[i].Name == name {
			return &(*c)[i]
		}
	}
	return nil
}

func (c *Columns) GetColumns()[]string {
	result := []string{}
	for _, column := range (*c) {
		result = append(result, column.Name)
	}
	return result
}

func (c *Columns) GetRowsCount() int {
	return len((*c)[0].bytes) / (*c)[0].Length
}

func (c *Columns) GetRowByIndex(index int) map[string][]byte {
	res := map[string][]byte{}
	for i := 0; i < len(*c); i++ {
		res[(*c)[i].Name] = (*c)[i].GetBytes(index)
	}
	return res
}

func (c *Columns) GetValue(position int, column string) interface{} {
	return c.ByName(column).GetValue(position)
}

func (c *Columns) GetBytes(position int, column string) []byte {
	return c.ByName(column).GetBytes(position)
}

func (c *Columns) SetBytes(position int, column string, value []byte) {
	c.ByName(column).SetBytes(position, value)
}

