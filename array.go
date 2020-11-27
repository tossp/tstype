package tstype

import (
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
)

var quoteArrayReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func quoteArrayElement(src string) string {
	return `"` + quoteArrayReplacer.Replace(src) + `"`
}
func findDimensionsFromValue(value reflect.Value, dimensions []pgtype.ArrayDimension, elementsLength int) ([]pgtype.ArrayDimension, int, bool) {
	switch value.Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		length := value.Len()
		if 0 == elementsLength {
			elementsLength = length
		} else {
			elementsLength *= length
		}
		dimensions = append(dimensions, pgtype.ArrayDimension{Length: int32(length), LowerBound: 1})
		for i := 0; i < length; i++ {
			if d, l, ok := findDimensionsFromValue(value.Index(i), dimensions, elementsLength); ok {
				return d, l, true
			}
		}
	}
	return dimensions, elementsLength, true
}
