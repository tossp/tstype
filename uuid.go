package tstype

import (
	"bytes"
	"database/sql/driver"

	errors "golang.org/x/xerrors"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgtype"
)

type UUID struct {
	UUID   uuid.UUID
	Status Status
}

func (dst *UUID) Set(src interface{}) error {
	if src == nil {
		*dst = UUID{Status: Null}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case uuid.UUID:
		*dst = UUID{UUID: value, Status: Present}
	case [16]byte:
		*dst = UUID{UUID: uuid.UUID(value), Status: Present}
	case []byte:
		if len(value) != 16 {
			return errors.Errorf("[]byte must be 16 bytes to convert to UUID: %d", len(value))
		}
		*dst = UUID{Status: Present}
		copy(dst.UUID[:], value)
	case string:
		uuid, err := uuid.FromString(value)
		if err != nil {
			return err
		}
		*dst = UUID{UUID: uuid, Status: Present}
	default:
		// If all else fails see if pgtype.UUID can handle it. If so, translate through that.
		pgUUID := &UUID{}
		if err := pgUUID.Set(value); err != nil {
			return errors.Errorf("cannot convert %#v to UUID", value)
		}

		*dst = UUID{UUID: uuid.UUID(pgUUID.UUID), Status: pgUUID.Status}
	}

	return nil
}

func (dst UUID) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.UUID
	default:
		return nil
	}
}

func (src *UUID) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *uuid.UUID:
			*v = src.UUID
			return nil
		case *[16]byte:
			*v = [16]byte(src.UUID)
			return nil
		case *[]byte:
			*v = make([]byte, 16)
			copy(*v, src.UUID[:])
			return nil
		case *string:
			*v = src.UUID.String()
			return nil
		default:
			if nextDst, retry := pgtype.GetAssignToDstType(v); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return pgtype.NullAssignTo(dst)
	}

	return errors.Errorf("cannot assign %#v into %T", src, dst)
}

func (dst *UUID) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = UUID{Status: Null}
		return nil
	}

	u, err := uuid.FromString(string(src))
	if err != nil {
		return err
	}

	*dst = UUID{UUID: u, Status: Present}
	return nil
}

func (dst *UUID) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = UUID{Status: Null}
		return nil
	}

	if len(src) != 16 {
		return errors.Errorf("invalid length for UUID: %v", len(src))
	}

	*dst = UUID{Status: Present}
	copy(dst.UUID[:], src)
	return nil
}

func (src UUID) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	}

	return append(buf, src.UUID.String()...), nil
}

func (src UUID) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	}

	return append(buf, src.UUID[:]...), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *UUID) Scan(src interface{}) error {
	if src == nil {
		*dst = UUID{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src UUID) Value() (driver.Value, error) {
	return pgtype.EncodeValueText(src)
}

func (src UUID) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Present:
		return []byte(`"` + src.UUID.String() + `"`), nil
	case Null:
		return []byte("null"), nil
	}

	return nil, errBadStatus
}

func (dst *UUID) UnmarshalJSON(b []byte) (err error) {
	if b == nil || string(b) == "null" {
		*dst = UUID{Status: Null}
		return
	}
	u := uuid.NullUUID{}
	if len(b) == 16 {
		err = u.UUID.UnmarshalBinary(b)
	} else {
		b = bytes.Trim(b, "\"")
		err = u.UUID.UnmarshalText(b)
	}

	if err != nil {
		return
	}
	*dst = UUID{UUID: u.UUID, Status: Present}
	return
}
