package qselect

import (
	"strconv"
	"ddb/structs/types"
)

func ParseLimit(q string) (query string, limit types.Limit, result bool) {

	if result = relimit.MatchString(q); result {

		submatch := relimit.FindAllStringSubmatch(q, 5)

		if len(submatch[0][3]) > 0 {
			limit.Offset, _ = strconv.Atoi(submatch[0][2])
			limit.RowCount, _ = strconv.Atoi(submatch[0][3])
		} else {
			limit.RowCount, _ = strconv.Atoi(submatch[0][2])
		}
		q = string(relimit.ReplaceAll([]byte(q), []byte{}))
	}

	return q, limit, result
}
