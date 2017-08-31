package cdriver

import (
	"io"
	"os"
)

type Column struct {
	Name         string `json:"name"`
	Title        string `json:"title"`
	Length       int    `json:"length"`
	Type         string `json:"type"`
	LastSavedPos int    `json:"last_save_pos"`
	table        *Table
	bytes        []byte
}

func (c *Column) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + c.table.Name + ".c" + c.Name
}

func (c *Column) Init(t *Table) {
	c.table = t
}

func (c *Column) SetValue(index int, val interface{}) {
	value, err := ValueToBytes(val, c.Length)
	if err != nil {
		panic(err)
	}
	c.SetBytes(index, value)
}

func (c *Column) SetBytes(index int, value []byte) {
	count := len(c.bytes) / c.Length

	if index >= count {
		c.bytes = append(c.bytes, make([]byte, (count-index+1)*c.Length)...)
	}

	for i := 0; i < c.Length; i++ {
		c.bytes[index*c.Length+i] = value[i]
	}
}

func (c *Column) GetBytes(index int) ([]byte, bool) {
	count := len(c.bytes) / c.Length
	if index > -1 && index < count {
		return c.bytes[index*c.Length : (index+1)*c.Length], true
	}
	return nil, false
}

func (c *Column) Load() error {
	c.bytes = []byte{}

	f, err := os.OpenFile(c.GetFileName(), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	bb := make([]byte, c.Length*1000)

	for {
		if _, err := f.Read(bb); err != nil {
			if err == io.EOF {
				break
			}
			return err
		} else {
			c.bytes = append(c.bytes, bb...)
		}
	}
	return nil
}

func (c *Column) Save() error {
	f, err := os.OpenFile(c.GetFileName(), os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Seek(int64(c.LastSavedPos)*int64(c.Length), 0); err != nil {
		return err
	}

	if _, err := f.Write(c.bytes); err != nil {
		return err
	}

	return f.Sync()
}
