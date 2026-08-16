package errors

import "errors"

// ErrNotFound signals a lookup/update hit no record, so repositories return
// it instead of (nil, nil) — "nothing happened" stays a checkable outcome.
var ErrNotFound = errors.New("not found")

// ErrForbidden: no cached driver mapping for the caller's X-User-Id — unlike
// matching-service's error there's no self-asserted id to check. Maps to 403.
var ErrForbidden = errors.New("no driver associated with this caller")

// ErrInvalidInput signals a malformed request the caller can fix (e.g. a
// coordinate out of range on a query endpoint). HTTP layer maps it to 400.
var ErrInvalidInput = errors.New("invalid input")
