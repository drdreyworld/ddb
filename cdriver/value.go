package cdriver

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"errors"
)

const ERR_BYTES_TO_LONG = "Length of bytes more than length of column"

func ValueToBytes(cell interface{}, length int) ([]byte, error) {
	buf := bytes.Buffer{}
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
	buf := bytes.Buffer{}
	buf.Write(b)

	dec := gob.NewDecoder(&buf)
	return dec.DecodeValue(v)
}

func DecodeValue(b []byte, res interface{}) error {
	buf := bytes.Buffer{}
	buf.Write(b)

	dec := gob.NewDecoder(&buf)
	err := dec.Decode(res)

	return err
}

func DecodeValueInt(b []byte) (res int, err error) {
	err = DecodeValue(b, &res)
	return res, err
}

func DecodeValueStr(b []byte) (res string, err error) {
	err = DecodeValue(b, &res)
	return res, err
}