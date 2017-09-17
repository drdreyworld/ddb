package funcs

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
	//return []byte{
	//	byte(i >> 24),
	//	byte(i >> 16),
	//	byte(i >> 8),
	//	byte(i),
	//}
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
		//byte(i),
		//byte(i >> 8),
		//byte(i >> 16),
		//byte(i >> 24),
	}
}

func Int32FromBytes(b []byte) int32 {
	//return int32(uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24)
	//i := int32(uint32(b[1]) | uint32(b[2])<<8 | uint32(b[3])<<16 | uint32(b[4])<<24)
	i := int32(uint32(b[4]) | uint32(b[3])<<8 | uint32(b[2])<<16 | uint32(b[1])<<24)
	if b[0] == 0 {
		i = -i
	}
	return i
}

func ValueToBytes(v interface{}) []byte {
	switch v.(type) {
	case int:
		return Int32ToBytes(int32(v.(int)))
	case int32:
		return Int32ToBytes(v.(int32))
	case string:
		return StringToNullByte(v.(string))
	}
	return []byte{}
}