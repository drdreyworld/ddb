package cdriver

import (
	"io"
	"os"
)

type ColumnOld struct {
	Name         string `json:"name"`
	Title        string `json:"title"`
	Length       int    `json:"length"`
	Type         string `json:"type"`
	LastSavedPos int    `json:"last_save_pos"`
	table        *Table
	values       []Value
}

func (c *ColumnOld) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + c.table.Name + ".c" + c.Name
}

func (c *ColumnOld) Init(t *Table) {
	c.table = t
	c.values = []Value{}
}

func (c *ColumnOld) SetValue(pos int, val interface{}) {
	length := len(c.values)
	value, err := ValueToBytes(val, c.Length)

	if err != nil {
		panic(err)
	}

	if length == pos {
		c.values = append(c.values, value)
	} else if length > pos {
		c.values[pos] = value
	} else {
		c.values = append(c.values, make([]Value, pos-length)...)
		c.values = append(c.values, value)
	}
}

func (c *ColumnOld) SetBytes(pos int, value []byte) {
	length := len(c.values)

	if length == pos {
		c.values = append(c.values, value)
	} else if length > pos {
		c.values[pos] = value
	} else {
		c.values = append(c.values, make([]Value, pos-length)...)
		c.values = append(c.values, value)
	}
}

func (c *ColumnOld) GetValue(pos int) (Value, bool) {
	if pos <= len(c.values) {
		return c.values[pos], true
	}
	return nil, false
}

func (c *ColumnOld) Load() error {
	c.values = []Value{}

	f, err := os.OpenFile(c.GetFileName(), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	bb := make([]byte, c.Length*1000)

	i := 0
	for {
		if n, err := f.Read(bb); err != nil {
			if err == io.EOF {
				break
			}
			return err
		} else {
			for j := 0; j < n/c.Length; j++ {
				//fmt.Println(bb[j*c.Length:(j+1)*c.Length])
				c.values = append(c.values, bb[j*c.Length:(j+1)*c.Length])
				//c.index.Add(bb[j*c.Length:(j+1)*c.Length], i)
				i++
			}
		}
	}
	//b := make([]byte, c.Length)
	//
	//for r := 0; ; r++ {
	//	b := make([]byte, c.Length)
	//	if _, err := f.Read(b); err != nil {
	//		if err == io.EOF {
	//			break
	//		}
	//		return err
	//	} else {
	//		c.values = append(c.values, b)
	//		c.index.Add(b, r)
	//	}
	//}
	return nil
}

func (c *ColumnOld) Save() error {
	f, err := os.OpenFile(c.GetFileName(), os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(int64(c.LastSavedPos)*int64(c.Length), 0); err != nil {
		return err
	}

	for _, value := range c.values {
		if _, err := f.Write(value); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	c.LastSavedPos = len(c.values)

	return nil
}
