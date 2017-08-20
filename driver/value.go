package driver

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

type Value []byte

func ValueToBytes(cell interface{}, length int) ([]byte, error) {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cell)
	if err != nil {
		return nil, err
	}
	b1 := buf.Bytes()

	if len(b1) > length {
		panic(fmt.Sprintf("bytes length greather than length:", len(b1), ">", length, "\n"))
	}

	if length > len(b1) {
		b1 = append(make([]byte, length-len(b1)), b1...)
	}

	return b1, nil
}

func ValueFromBytes(b []byte, v reflect.Value) error {

	for j := 0; j < len(b); j++ {
		if b[j] > 0 {
			b = b[j:]
			break
		}
	}

	var buf bytes.Buffer
	buf.Write(b)

	dec := gob.NewDecoder(&buf)
	return dec.DecodeValue(v)
}