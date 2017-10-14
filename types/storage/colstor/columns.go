package colstor

import (
	"ddb/types/config"
	"strconv"
	"sync"
	"time"
	"fmt"
)

type Columns []Column

func (c *Columns) Init(tableName string, cfg config.ColumnsConfig) {
	for _, columnConfig := range cfg {
		*c = append(*c, Column{
			Name:   columnConfig.Name,
			Length: columnConfig.Length,
			Type:   columnConfig.Type,
			Table:  tableName,
		})
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

func (c *Columns) Flush() error {
	t := time.Now()
	fmt.Print("Flush columns: ")
	var mutex sync.Mutex
	mutex.Lock()
	for i := range *c {
		if err := (*c)[i].Flush(); err != nil {
			return err
		}
	}
	mutex.Unlock()
	fmt.Print(time.Now().Sub(t), "\n")
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

func (c *Columns) GetColumnsConfig() config.ColumnsConfig {
	result := config.ColumnsConfig{}
	for _, column := range *c {
		result = append(result, config.ColumnConfig{
			Name:   column.Name,
			Length: column.Length,
			Type:   column.Type,
		})
	}
	return result
}

func (c *Columns) GetColumns() []string {
	result := []string{}
	for _, column := range *c {
		result = append(result, column.Name)
	}
	return result
}

func (c *Columns) GetRowsCount() int {
	return len((*c)[0].bytes) / (*c)[0].Length
}

func (c *Columns) GetRowStringMapByIndex(index int) map[string]string {
	res := map[string]string{}
	for i := 0; i < len(*c); i++ {
		switch v := c.GetValue(index, ((*c)[i]).Name).(type) {
		case string:
			res[(*c)[i].Name] = v
			break
		case int32:
			res[(*c)[i].Name] = strconv.Itoa(int(v))
			break
		case int:
			res[(*c)[i].Name] = strconv.Itoa(v)
			break
		default:
			panic("Unknown value type")
		}
	}
	return res
}

func (c *Columns) GetRowBytesByIndex(index int) map[string][]byte {
	res := map[string][]byte{}
	for i := 0; i < len(*c); i++ {
		res[(*c)[i].Name] = (*c)[i].GetBytes(index)
	}
	return res
}

func (c *Columns) GetValueByColumnIndex(position int, columnIndex int) interface{} {
	return (*c)[columnIndex].GetValue(position)
}

func (c *Columns) GetBytesByColumnIndex(position int, columnIndex int) []byte {
	return (*c)[columnIndex].GetBytes(position)
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

