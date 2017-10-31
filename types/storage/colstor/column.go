package colstor

import (
	"os"
	"sync"
	"github.com/drdreyworld/smconv"
)

type Column struct {
	Name   string
	Length int
	Type   string
	Table  string
	bytes  []byte
	changed map[int]bool
}

func (c *Column) GetFileName() string {
	return "/Users/andrey/Go/src/ddb/data/t" + c.Table + ".c" + c.Name
}

func (c *Column) SetValue(index int, val interface{}) {
	c.SetBytes(index, smconv.ValueToBytes(val, c.Length))
}

func (c *Column) SetBytes(index int, value []byte) {
	count := len(c.bytes) / c.Length

	if index >= count {
		c.bytes = append(c.bytes, make([]byte, (count-index+1)*c.Length)...)
	}

	copy(c.bytes[index*c.Length:], value)

	var mutex sync.Mutex

	mutex.Lock()
	if c.changed == nil {
		c.changed = map[int]bool{}
	}
	c.changed[index] = true
	mutex.Unlock()
}

func (c *Column) GetBytes(index int) []byte {
	return c.bytes[index*c.Length : (index+1)*c.Length]
}

func (c *Column) GetValue(index int) interface{} {
	switch c.Type {
	case "string":
		return smconv.StringFromNullByte(c.GetBytes(index))
		break
	case "int32":
		return smconv.Int32FromBytes(c.GetBytes(index))
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

	if _, err := f.Write(c.bytes); err != nil {
		return err
	}

	return f.Sync()
}


func (c *Column) Flush() error {
	if c.changed == nil {
		return nil
	}

	f, err := os.OpenFile(c.GetFileName(), os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	for pos := range c.changed {
		f.Seek(int64(pos * c.Length), 0)
		if _, err := f.Write(c.GetBytes(pos)); err != nil {
			return err
		}
	}

	return f.Sync()
}
