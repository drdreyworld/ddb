package key

const (
	CMP_KEY_EQUAL    = 0
	CMP_KEY_LESS     = -1
	CMP_KEY_GREATHER = 1
)

type BytesKey []byte

func (k BytesKey) Equal(key BytesKey) (r bool) {
	if len(k) != len(key) {
		return false
	}

	l := len(key)

	for i := 0; i < l; i++ {
		if k[i] != (key)[i] {
			return false
		}
	}

	return true
}

func (k BytesKey) Less(key BytesKey) (r bool) {
	return k.Compare(key) == CMP_KEY_LESS
}

func (k BytesKey) Greather(key BytesKey) (r bool) {
	return k.Compare(key) == CMP_KEY_GREATHER
}

func (k BytesKey) Compare(key BytesKey) (r int) {
	lk := len(k)
	lK := len(key)

	l := lk

	if lK > l {
		l = lK
	}

	var a, b byte

	for i := 0; i < l; i++ {
		if i >= lk {
			a = 0
		} else {
			a = (k)[i]
		}

		if i >= lK {
			b = 0
		} else {
			b = (key)[i]
		}

		if a < b {
			return CMP_KEY_LESS
		} else if a > b {
			return CMP_KEY_GREATHER
		}
	}

	return CMP_KEY_EQUAL
}
