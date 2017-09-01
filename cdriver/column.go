package cdriver

import (
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

	copy(c.bytes[index*c.Length:], value)
}

func (c *Column) GetBytes(index int) ([]byte) {
	return c.bytes[index*c.Length : (index+1)*c.Length]
}

func (c *Column) Load() error {
	c.bytes = []byte{}

	f, err := os.OpenFile(c.GetFileName(), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	s, err := f.Stat()
	if err != nil {
		return err
	}

	c.bytes = make([]byte, s.Size())
	if _, err := f.Read(c.bytes); err != nil {
		return err
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
