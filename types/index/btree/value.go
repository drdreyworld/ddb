package btree

import (
	"ddb/types/key"
)

type Value struct {
	Key  key.BytesKey
	Data []int
	Tree *BTree
}

type Values []*Value