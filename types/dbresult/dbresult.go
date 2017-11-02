package dbresult

import (
	"ddb/storage/engine"
	"errors"
)

type DbResult struct {
	storage   engine.Storage
	current   int
	positions []int
}

func (r *DbResult) Init(storage engine.Storage) {
	r.storage = storage
}

func (r *DbResult) SetPositions(p []int) {
	r.current = 0
	r.positions = p
}

func (r *DbResult) GetRowsCount() int {
	return len(r.positions)
}

func (r *DbResult) Rewind() {
	r.current = 0
}

func (r *DbResult) FetchMap() (map[string]string, error) {
	if r.current >= len(r.positions) {
		return nil, errors.New("EOF")
	}

	r.current++

	return r.storage.GetRowStringMapByIndex(r.positions[r.current-1]), nil
}
