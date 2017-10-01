package btree

import (
	"ddb/types/key"
)

type Data interface{}

type Value struct {
	Key  key.BytesKey
	Data Data
}

type Values []*Value