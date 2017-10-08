package key

import "bytes"

const (
	CMP_KEY_EQUAL    = 0
	CMP_KEY_LESS     = -1
	CMP_KEY_GREATHER = 1
)

type BytesKey []byte

func (k BytesKey) Equal(key BytesKey) (r bool) {
	return k.Compare(key) == CMP_KEY_EQUAL
}

func (k BytesKey) Less(key BytesKey) (r bool) {
	return k.Compare(key) == CMP_KEY_LESS
}

func (k BytesKey) LessOrEqual(key BytesKey) (r bool) {
	return k.Compare(key) <= CMP_KEY_EQUAL
}

func (k BytesKey) Greather(key BytesKey) (r bool) {
	return k.Compare(key) == CMP_KEY_GREATHER
}

func (k BytesKey) GreatherOrEqual(key BytesKey) (r bool) {
	return k.Compare(key) >= CMP_KEY_EQUAL
}

func (k BytesKey) Compare(key BytesKey) (r int) {
	return bytes.Compare(k, key)
}
