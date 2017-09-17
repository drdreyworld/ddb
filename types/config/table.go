package config


type TableConfig struct {

	Name string `json:"name"`

	Storage string `json:"storage"`

	IndexType string `json:"indexType"`

	Columns ColumnsConfig `json:"columns"`

	MaxId int `json:"max_id"`

	Indexes IndexesConfig `json:"indexes"`
}

