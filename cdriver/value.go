package cdriver

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"errors"
)

const ERR_BYTES_TO_LONG = "Length of bytes more than length of column"

func ValueToBytes(cell interface{}, length int) ([]byte, error) {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cell)
	if err != nil {
		return nil, err
	}
	b1 := buf.Bytes()

	if len(b1) > length {
		return nil, errors.New(ERR_BYTES_TO_LONG)
	}

	if length > len(b1) {
		b1 = append(b1, make([]byte, length-len(b1))...)
	}

	return b1, nil
}

func ValueFromBytes(b []byte, v reflect.Value) error {
	var buf bytes.Buffer
	buf.Write(b)

	dec := gob.NewDecoder(&buf)
	return dec.DecodeValue(v)
}