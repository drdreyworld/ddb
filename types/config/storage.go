package config

type ColumnConfig struct {
	Name   string `json:"name"`
	Length int    `json:"length"`
	Type   string `json:"type"`
}

type ColumnsConfig []ColumnConfig

func (cc *ColumnsConfig) ByName(column string) *ColumnConfig {
	for i := range *cc {
		if (*cc)[i].Name == column {
			return &(*cc)[i]
		}
	}
	return nil
}
