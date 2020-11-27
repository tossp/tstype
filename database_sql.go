package tstype

import (
	"database/sql/driver"

	"github.com/jackc/pgtype"

	errors "golang.org/x/xerrors"
)

func DatabaseSQLValue(ci *pgtype.ConnInfo, src pgtype.Value) (interface{}, error) {
	if valuer, ok := src.(driver.Valuer); ok {
		return valuer.Value()
	}

	if textEncoder, ok := src.(pgtype.TextEncoder); ok {
		buf, err := textEncoder.EncodeText(ci, nil)
		if err != nil {
			return nil, err
		}
		return string(buf), nil
	}

	if binaryEncoder, ok := src.(pgtype.BinaryEncoder); ok {
		buf, err := binaryEncoder.EncodeBinary(ci, nil)
		if err != nil {
			return nil, err
		}
		return buf, nil
	}

	return nil, errors.New("cannot convert to database/sql compatible value")
}

func EncodeValueText(src pgtype.TextEncoder) (interface{}, error) {
	buf, err := src.EncodeText(nil, make([]byte, 0, 32))
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return string(buf), err
}
