package tstype

import (
	"database/sql/driver"
	"strconv"

	errors "golang.org/x/xerrors"

	"github.com/jackc/pgtype"
	"github.com/shopspring/decimal"
)

type Numeric struct {
	Decimal decimal.Decimal
	Status  Status
}

func (dst *Numeric) Set(src interface{}) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case decimal.Decimal:
		*dst = Numeric{Decimal: value, Status: Present}
	case float32:
		*dst = Numeric{Decimal: decimal.NewFromFloat(float64(value)), Status: Present}
	case float64:
		*dst = Numeric{Decimal: decimal.NewFromFloat(value), Status: Present}
	case int8:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case uint8:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case int16:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case uint16:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case int32:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case uint32:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case int64:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case uint64:
		// uint64 could be greater than int64 so convert to string then to decimal
		dec, err := decimal.NewFromString(strconv.FormatUint(value, 10))
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Status: Present}
	case int:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Status: Present}
	case uint:
		// uint could be greater than int64 so convert to string then to decimal
		dec, err := decimal.NewFromString(strconv.FormatUint(uint64(value), 10))
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Status: Present}
	case string:
		dec, err := decimal.NewFromString(value)
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Status: Present}
	default:
		// If all else fails see if pgtype.Numeric can handle it. If so, translate through that.
		num := &pgtype.Numeric{}
		if err := num.Set(value); err != nil {
			return errors.Errorf("cannot convert %v to Numeric", value)
		}

		buf, err := num.EncodeText(nil, nil)
		if err != nil {
			return errors.Errorf("cannot convert %v to Numeric", value)
		}

		dec, err := decimal.NewFromString(string(buf))
		if err != nil {
			return errors.Errorf("cannot convert %v to Numeric", value)
		}
		*dst = Numeric{Decimal: dec, Status: Present}
	}

	return nil
}

func (dst Numeric) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Decimal
	default:
		return nil
	}
}

func (src *Numeric) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *decimal.Decimal:
			*v = src.Decimal
		case *float32:
			f, _ := src.Decimal.Float64()
			*v = float32(f)
		case *float64:
			f, _ := src.Decimal.Float64()
			*v = f
		case *int:
			if src.Decimal.Exponent() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseInt(src.Decimal.String(), 10, strconv.IntSize)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = int(n)
		case *int8:
			if src.Decimal.Exponent() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseInt(src.Decimal.String(), 10, 8)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = int8(n)
		case *int16:
			if src.Decimal.Exponent() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseInt(src.Decimal.String(), 10, 16)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = int16(n)
		case *int32:
			if src.Decimal.Exponent() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseInt(src.Decimal.String(), 10, 32)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = int32(n)
		case *int64:
			if src.Decimal.Exponent() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseInt(src.Decimal.String(), 10, 64)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = int64(n)
		case *uint:
			if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseUint(src.Decimal.String(), 10, strconv.IntSize)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = uint(n)
		case *uint8:
			if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseUint(src.Decimal.String(), 10, 8)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = uint8(n)
		case *uint16:
			if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseUint(src.Decimal.String(), 10, 16)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = uint16(n)
		case *uint32:
			if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseUint(src.Decimal.String(), 10, 32)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = uint32(n)
		case *uint64:
			if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			n, err := strconv.ParseUint(src.Decimal.String(), 10, 64)
			if err != nil {
				return errors.Errorf("cannot convert %v to %T", dst, *v)
			}
			*v = uint64(n)
		default:
			if nextDst, retry := pgtype.GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return NullAssignTo(dst)
	}

	return nil
}

func (dst *Numeric) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	dec, err := decimal.NewFromString(string(src))
	if err != nil {
		return err
	}

	*dst = Numeric{Decimal: dec, Status: Present}
	return nil
}

func (dst *Numeric) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	// For now at least, implement this in terms of pgtype.Numeric

	num := &pgtype.Numeric{}
	if err := num.DecodeBinary(ci, src); err != nil {
		return err
	}

	*dst = Numeric{Decimal: decimal.NewFromBigInt(num.Int, num.Exp), Status: Present}

	return nil
}

func (src Numeric) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Present:
		return append(buf, src.Decimal.String()...), nil
	case Null:
		return nil, nil
	default:
		return nil, errBadStatus
	}
}

func (src Numeric) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Present:
		// For now at least, implement this in terms of pgtype.Numeric
		num := &pgtype.Numeric{}
		if err := num.DecodeText(ci, []byte(src.Decimal.String())); err != nil {
			return nil, err
		}

		return num.EncodeBinary(ci, buf)
	case Null:
		return nil, nil
	default:
		return nil, errBadStatus
	}
}

// Scan implements the database/sql Scanner interface.
func (dst *Numeric) Scan(src interface{}) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case float64:
		*dst = Numeric{Decimal: decimal.NewFromFloat(src), Status: Present}
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Numeric) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		return src.Decimal.Value()
	case Null:
		return nil, nil
	default:
		return nil, errBadStatus
	}
}

func (src Numeric) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Present:
		return src.Decimal.MarshalJSON()
	case Null:
		return []byte("null"), nil
	}
	return nil, errBadStatus
}

func (dst *Numeric) UnmarshalJSON(b []byte) error {
	d := decimal.NullDecimal{}
	err := d.UnmarshalJSON(b)
	if err != nil {
		return err
	}

	status := Null
	if d.Valid {
		status = Present
	}
	*dst = Numeric{Decimal: d.Decimal, Status: status}

	return nil
}
