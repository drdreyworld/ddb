package config

import (
	"io/ioutil"
	"encoding/json"
)

type TableConfig struct {

	Name string `json:"name"`

	Storage string `json:"storage"`

	IndexType string `json:"indexType"`

	Columns ColumnsConfig `json:"columns"`

	MaxId int `json:"max_id"`

	Indexes IndexesConfig `json:"indexes"`
}

func (tc *TableConfig) Load(filename string) error {
	if data, err := ioutil.ReadFile(filename); err != nil {
		return err
	} else {
		return json.Unmarshal(data, tc)
	}
}

func (tc *TableConfig) Save(filename string) error {
	if data, err := json.Marshal(tc); err != nil {
		return err
	} else {
		return ioutil.WriteFile(filename, data, 0777)
	}
}