package table

import (
	"ddb/storage/config"
	"ddb/storage/engine"
	sbptree "ddb/storage/engine/bptree"
	ibptree "ddb/storage/index/bptree"
	"ddb/storage/index"
)

type Table struct {
	config  config.TableConfig
	storage engine.Storage
	indexes index.Indexes
}

func (t *Table) Open(config config.TableConfig) {
	t.config = config

	t.storage = &sbptree.Storage{}
	t.storage.Open(t.config.Name, t.config.Columns)

	for _, cfg := range t.config.Indexes {
		t.indexes = append(t.indexes, t.CreateIndex(cfg))
	}
}

func (t *Table) CreateIndex(cfg config.IndexConfig) (idx index.Index) {
	idx = &ibptree.Index{}
	idx.Open(cfg)
	return
}

func (t *Table) Close() {
	t.storage.Close()

	for i := range t.indexes {
		t.indexes[i].Close()
	}
}