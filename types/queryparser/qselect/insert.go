package qselect

import (
	"ddb/types/query"
	"errors"
)

func CreateInsertFromString(q string) (*query.Insert, error) {
	var b bool

	if _, q, b = matchAndReplace(reinsert, q); !b {
		return nil, nil
	}

	result := &query.Insert{}

	// @TODO

	if len(q) > 0 {
		return nil, errors.New("parse error near " + q)
	}

	return result, nil
}