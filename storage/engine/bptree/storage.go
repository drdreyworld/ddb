package bptree

import (
	"ddb/storage/config"
	"github.com/drdreyworld/bptree"
	"github.com/drdreyworld/smconv"
	"strconv"
)

type Storage struct {
	index    bptree.Tree
	config   config.ColumnsConfig
	keySize  int
	dataSize int
}

func (c *Storage) Open(tableName string, cfg config.ColumnsConfig) {
	c.config = cfg

	filename := "/Users/andrey/Go/src/ddb/data/bpt." + tableName + ".idx"

	// uint32
	c.keySize = 4
	// dataSize - sum of row field lengths
	c.dataSize = 0
	for i := 0; i < len(cfg); i++ {
		c.dataSize += cfg[i].Length
	}

	c.index = bptree.Tree{}
	c.index.Init(c.keySize, c.dataSize)
	c.index.Mid = 20

	if err := c.index.OpenFile(filename); err != nil {
		panic(err)
	}
}

func (c *Storage) Close() error {
	return c.index.CloseFile()
}

func (c *Storage) GetColumnsConfig() config.ColumnsConfig {
	return c.config
}

func (c *Storage) GetColumns() []string {
	result := []string{}
	for _, column := range c.config {
		result = append(result, column.Name)
	}
	return result
}

func (c *Storage) GetRowsCount() int {
	return c.index.GetRowsCount()
}

func (c *Storage) GetRowStringMapByIndex(index int) map[string]string {
	row := c.index.Find(smconv.Uint32ToBytes(uint32(index)))
	if row == nil {
		return nil
	}
	data := (*row)[c.keySize+4+4:]

	res := map[string]string{}
	pos := 0

	for i := 0; i < len(c.config); i++ {
		value := data[pos : pos+c.config[i].Length]
		switch c.config[i].Type {
		case "int32":
			res[c.config[i].Name] = strconv.Itoa(int(smconv.Int32FromBytes(value)))
		case "int":
			res[c.config[i].Name] = strconv.Itoa(int(smconv.Int32FromBytes(value)))
		case "string":
			res[c.config[i].Name] = smconv.StringFromNullByte(value)
		}

		pos += c.config[i].Length
	}
	return res
}

func (c *Storage) GetRowBytesByIndex(index int) map[string][]byte {
	row := c.index.Find(smconv.Uint32ToBytes(uint32(index)))
	if row == nil {
		return nil
	}
	data := (*row)[c.keySize+4+4:]

	res := map[string][]byte{}
	pos := 0

	for i := 0; i < len(c.config); i++ {
		res[c.config[i].Name] = data[pos : pos+c.config[i].Length]
		pos += c.config[i].Length
	}
	return res
}

func (c *Storage) SetRowBytesByIndex(index int, values map[string][]byte) {
	key := smconv.Uint32ToBytes(uint32(index))
	row := c.index.Find(key)

	if row == nil {
		row = bptree.CreateRow(
			c.keySize,
			c.dataSize,
			key,
			[]byte{},
		)
	}

	pos := c.keySize + 4 + 4
	for i := 0; i < len(c.config); i++ {
		if value, ok := values[c.config[i].Name]; ok {
			copy((*row)[pos:pos+c.config[i].Length], value)
		}
		pos += c.config[i].Length
	}
	c.index.Insert(row)
}

func (c *Storage) GetValueByColumnIndex(position int, columnIndex int) interface{} {
	row := c.GetRowStringMapByIndex(position)
	col := c.config[columnIndex].Name
	return row[col]
}

func (c *Storage) GetBytesByColumnIndex(position int, columnIndex int) []byte {
	row := c.GetRowBytesByIndex(position)
	col := c.config[columnIndex].Name
	return row[col]
}

func (c *Storage) GetBytes(position int, column string) []byte {
	row := c.GetRowBytesByIndex(position)
	return row[column]
}

func (c *Storage) SetBytes(position int, column string, value []byte) {
	key := smconv.Uint32ToBytes(uint32(position))
	row := c.index.Find(key)

	if row == nil {
		row = bptree.CreateRow(
			c.keySize,
			c.dataSize,
			key,
			[]byte{},
		)
	}

	pos := c.keySize + 4 + 4
	for i := 0; i < len(c.config); i++ {
		if c.config[i].Name == column {
			copy((*row)[pos:pos+c.config[i].Length], value)
			break
		}
		pos += c.config[i].Length
	}
	c.index.Insert(row)
}
