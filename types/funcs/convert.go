package funcs

import (
	"errors"
)

const ERR_UNKNOWN_TYPE = "Unknown value type"
const ERR_BYTES_TO_LONG = "Length of bytes more than length of column"

func StringToNullByte(s string) (b []byte) {
	return append([]byte(s), 0)
}

func StringFromNullByte(b []byte) string {
	i := 0
	for i = 0; i < len(b); i++ {
		if b[i] == 0 {
			break;
		}
	}
	if b[i] == 0 {
		return string(b[:i])
	}
	return string(b)
}

func Int32ToBytes(i int32) []byte {
	var b byte = 0
	if i > -1 {
		b = 1
	} else {
		i = -i
	}

	return []byte{
		b,
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	}
}

func Int32FromBytes(b []byte) int32 {
	i := int32(uint32(b[4]) | uint32(b[3])<<8 | uint32(b[2])<<16 | uint32(b[1])<<24)
	if b[0] == 0 {
		i = -i
	}
	return i
}

func ValueToBytes(v interface{}, length int) ([]byte, error) {
	var b1 []byte

	switch v.(type) {
	case int:
		b1 = Int32ToBytes(int32(v.(int)))
	case int32:
		b1 = Int32ToBytes(v.(int32))
	case string:
		b1 = StringToNullByte(v.(string))
	default:
		return nil, errors.New(ERR_UNKNOWN_TYPE)
	}

	if len(b1) > length {
		return nil, errors.New(ERR_BYTES_TO_LONG)
	}

	if length > len(b1) {
		b1 = append(b1, make([]byte, length-len(b1))...)
	}

	return b1, nil
}