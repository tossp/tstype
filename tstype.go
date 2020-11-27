package tstype

import (
	"fmt"

	errors "golang.org/x/xerrors"
)

type Status byte

const (
	Null Status = iota
	Present
)

var errBadStatus = errors.New("invalid status")

type nullAssignmentError struct {
	dst interface{}
}

func (e *nullAssignmentError) Error() string {
	return fmt.Sprintf("cannot assign NULL to %T", e.dst)
}
