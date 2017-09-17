package colstor

import (
	"ddb/types/funcs"
	"os"
)

type Column struct {
	Name   string
	Length int
	Type   string
	Table  string
	bytes  []byte
}

func (c *Column) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + c.Table + ".c" + c.Name
}

func (c *Column) SetValue(index int, val interface{}) {
	if value, err := funcs.ValueToBytes(val, c.Length); err != nil {
		panic(err)
	} else {
		c.SetBytes(index, value)
	}
}

func (c *Column) SetBytes(index int, value []byte) {
	count := len(c.bytes) / c.Length

	if index >= count {
		c.bytes = append(c.bytes, make([]byte, (count-index+1)*c.Length)...)
	}

	copy(c.bytes[index*c.Length:], value)
}

func (c *Column) GetBytes(index int) []byte {
	return c.bytes[index*c.Length : (index+1)*c.Length]
}

func (c *Column) GetValue(index int) interface{} {
	switch c.Type {
	case "string":
		return funcs.StringFromNullByte(c.GetBytes(index))
		break
	case "int32":
		return funcs.Int32FromBytes(c.GetBytes(index))
		break
	}
	return nil
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

	//if _, err := f.Seek(int64(c.LastSavedPos)*int64(c.Length), 0); err != nil {
	//	return err
	//}

	if _, err := f.Write(c.bytes); err != nil {
		return err
	}

	return f.Sync()
}
