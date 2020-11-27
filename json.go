package tstype

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/jackc/pgtype"

	errors "golang.org/x/xerrors"
)

type JSON struct {
	Bytes  []byte
	Status Status
}

func (dst *JSON) Set(src interface{}) error {
	if src == nil {
		*dst = JSON{Status: Null}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case string:
		*dst = JSON{Bytes: []byte(value), Status: Present}
	case *string:
		if value == nil {
			*dst = JSON{Status: Null}
		} else {
			*dst = JSON{Bytes: []byte(*value), Status: Present}
		}
	case []byte:
		if value == nil {
			*dst = JSON{Status: Null}
		} else {
			*dst = JSON{Bytes: value, Status: Present}
		}
	// Encode* methods are defined on *JSON. If JSON is passed directly then the
	// struct itself would be encoded instead of Bytes. This is clearly a footgun
	// so detect and return an error. See https://github.com/jackc/pgx/issues/350.
	case JSON:
		return errors.New("use pointer to JSON instead of value")
	// Same as above but for JSONB (because they share implementation)
	case JSONB:
		return errors.New("use pointer to JSONB instead of value")

	default:
		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}
		*dst = JSON{Bytes: buf, Status: Present}
	}

	return nil
}

func (dst JSON) Get() interface{} {
	switch dst.Status {
	case Present:
		var i interface{}
		err := json.Unmarshal(dst.Bytes, &i)
		if err != nil {
			return dst
		}
		return i
	default:
		return nil
	}
}

func (src *JSON) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		if src.Status == Present {
			*v = string(src.Bytes)
		} else {
			return errors.Errorf("cannot assign non-present status to %T", dst)
		}
	case **string:
		if src.Status == Present {
			s := string(src.Bytes)
			*v = &s
			return nil
		} else {
			*v = nil
			return nil
		}
	case *[]byte:
		if src.Status != Present {
			*v = nil
		} else {
			buf := make([]byte, len(src.Bytes))
			copy(buf, src.Bytes)
			*v = buf
		}
	default:
		data := src.Bytes
		if data == nil || src.Status != Present {
			data = []byte("null")
		}

		return json.Unmarshal(data, dst)
	}

	return nil
}

func (JSON) PreferredResultFormat() int16 {
	return pgtype.TextFormatCode
}

func (dst *JSON) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = JSON{Status: Null}
		return nil
	}

	*dst = JSON{Bytes: src, Status: Present}
	return nil
}

func (dst *JSON) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	return dst.DecodeText(ci, src)
}

func (JSON) PreferredParamFormat() int16 {
	return pgtype.TextFormatCode
}

func (src JSON) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	}

	return append(buf, src.Bytes...), nil
}

func (src JSON) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return src.EncodeText(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *JSON) Scan(src interface{}) error {
	if src == nil {
		*dst = JSON{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src JSON) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		return src.Bytes, nil
	default:
		return nil, nil
	}
}

func (src JSON) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Present:
		return src.Bytes, nil
	case Null:
		return []byte("null"), nil
	}

	return nil, errBadStatus
}

func (dst *JSON) UnmarshalJSON(b []byte) error {
	if b == nil || string(b) == "null" {
		*dst = JSON{Status: Null}
	} else {
		*dst = JSON{Bytes: b, Status: Present}
	}
	return nil

}
