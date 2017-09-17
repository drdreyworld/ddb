package config

type IndexConfig struct {
	Name string   `json:"name"`
	Type string   `json:"type"`
	Cols []string `json:"columns"`
}

type IndexesConfig []IndexConfig
