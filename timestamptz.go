package tstype

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/jackc/pgtype"

	"github.com/jackc/pgio"
	errors "golang.org/x/xerrors"
)

const pgTimestamptzHourFormat = "2006-01-02 15:04:05.999999999Z07"
const pgTimestamptzMinuteFormat = "2006-01-02 15:04:05.999999999Z07:00"
const pgTimestamptzSecondFormat = "2006-01-02 15:04:05.999999999Z07:00:00"
const microsecFromUnixEpochToY2K = 946684800 * 1000000

const (
	negativeInfinityMicrosecondOffset = -9223372036854775808
	infinityMicrosecondOffset         = 9223372036854775807
)

type Timestamptz struct {
	Time             time.Time
	Status           Status
	InfinityModifier pgtype.InfinityModifier
}

func (dst *Timestamptz) Set(src interface{}) error {
	if src == nil {
		*dst = Timestamptz{Status: Null}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case time.Time:
		*dst = Timestamptz{Time: value, Status: Present}
	case *time.Time:
		if value == nil {
			*dst = Timestamptz{Status: Null}
		} else {
			return dst.Set(*value)
		}
	case pgtype.InfinityModifier:
		*dst = Timestamptz{InfinityModifier: value, Status: Present}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return dst.Set(originalSrc)
		}
		return errors.Errorf("cannot convert %v to Timestamptz", value)
	}

	return nil
}

func (dst Timestamptz) Get() interface{} {
	switch dst.Status {
	case Present:
		if dst.InfinityModifier != pgtype.None {
			return dst.InfinityModifier
		}
		return dst.Time
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Timestamptz) AssignTo(dst interface{}) error {
	switch src.Status {
	case Present:
		switch v := dst.(type) {
		case *time.Time:
			if src.InfinityModifier != pgtype.None {
				return errors.Errorf("cannot assign %v to %T", src, dst)
			}
			*v = src.Time
			return nil
		default:
			if nextDst, retry := GetAssignToDstType(dst); retry {
				return src.AssignTo(nextDst)
			}
			return errors.Errorf("unable to assign to %T", dst)
		}
	case Null:
		return NullAssignTo(dst)
	}

	return errors.Errorf("cannot decode %#v into %T", src, dst)
}

func (dst *Timestamptz) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamptz{Status: Null}
		return nil
	}

	sbuf := string(src)
	switch sbuf {
	case "infinity":
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.Infinity}
	case "-infinity":
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.NegativeInfinity}
	default:
		var format string
		if len(sbuf) >= 9 && (sbuf[len(sbuf)-9] == '-' || sbuf[len(sbuf)-9] == '+') {
			format = pgTimestamptzSecondFormat
		} else if len(sbuf) >= 6 && (sbuf[len(sbuf)-6] == '-' || sbuf[len(sbuf)-6] == '+') {
			format = pgTimestamptzMinuteFormat
		} else {
			format = pgTimestamptzHourFormat
		}

		tim, err := time.Parse(format, sbuf)
		if err != nil {
			return err
		}

		*dst = Timestamptz{Time: tim, Status: Present}
	}

	return nil
}

func (dst *Timestamptz) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Timestamptz{Status: Null}
		return nil
	}

	if len(src) != 8 {
		return errors.Errorf("invalid length for timestamptz: %v", len(src))
	}

	microsecSinceY2K := int64(binary.BigEndian.Uint64(src))

	switch microsecSinceY2K {
	case infinityMicrosecondOffset:
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.Infinity}
	case negativeInfinityMicrosecondOffset:
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.NegativeInfinity}
	default:
		microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
		tim := time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)
		*dst = Timestamptz{Time: tim, Status: Present}
	}

	return nil
}

func (src Timestamptz) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	}

	var s string

	switch src.InfinityModifier {
	case pgtype.None:
		s = src.Time.UTC().Truncate(time.Microsecond).Format(pgTimestamptzSecondFormat)
	case pgtype.Infinity:
		s = "infinity"
	case pgtype.NegativeInfinity:
		s = "-infinity"
	}

	return append(buf, s...), nil
}

func (src Timestamptz) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch src.Status {
	case Null:
		return nil, nil
	}

	var microsecSinceY2K int64
	switch src.InfinityModifier {
	case pgtype.None:
		microsecSinceUnixEpoch := src.Time.Unix()*1000000 + int64(src.Time.Nanosecond())/1000
		microsecSinceY2K = microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
	case pgtype.Infinity:
		microsecSinceY2K = infinityMicrosecondOffset
	case pgtype.NegativeInfinity:
		microsecSinceY2K = negativeInfinityMicrosecondOffset
	}

	return pgio.AppendInt64(buf, microsecSinceY2K), nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Timestamptz) Scan(src interface{}) error {
	if src == nil {
		*dst = Timestamptz{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return dst.DecodeText(nil, srcCopy)
	case time.Time:
		*dst = Timestamptz{Time: src, Status: Present}
		return nil
	}

	return errors.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Timestamptz) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		if src.InfinityModifier != pgtype.None {
			return src.InfinityModifier.String(), nil
		}
		return src.Time, nil
	default:
		return nil, nil
	}
}

func (src Timestamptz) MarshalJSON() ([]byte, error) {
	switch src.Status {
	case Null:
		return []byte("null"), nil
	}

	if src.Status != Present {
		return nil, errBadStatus
	}

	var s string

	switch src.InfinityModifier {
	case pgtype.None:
		s = src.Time.Format(time.RFC3339Nano)
	case pgtype.Infinity:
		s = "infinity"
	case pgtype.NegativeInfinity:
		s = "-infinity"
	}

	return json.Marshal(s)
}

func (dst *Timestamptz) UnmarshalJSON(b []byte) error {
	var s *string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	if s == nil {
		*dst = Timestamptz{Status: Null}
		return nil
	}

	switch *s {
	case "infinity":
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.Infinity}
	case "-infinity":
		*dst = Timestamptz{Status: Present, InfinityModifier: pgtype.NegativeInfinity}
	default:
		// PostgreSQL uses ISO 8601 for to_json function and casting from a string to timestamptz
		tim, err := time.Parse(time.RFC3339Nano, *s)
		if err != nil {
			return err
		}

		*dst = Timestamptz{Time: tim, Status: Present}
	}

	return nil
}
