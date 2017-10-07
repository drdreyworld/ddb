package config

type IndexConfig struct {
	Name string        `json:"name"`
	Type string        `json:"type"`
	Cols ColumnsConfig `json:"columns"`
}

type IndexesConfig []IndexConfig
