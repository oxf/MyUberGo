package errors

import "errors"

// ErrNotFound signals that a lookup or update targeted a record that doesn't
// exist. Repositories return it instead of (nil, nil) / a silently-ignored
// zero-rows-affected update, so "nothing happened" is a distinguishable,
// checkable (errors.Is) outcome rather than indistinguishable from success.
var ErrNotFound = errors.New("not found")

// ErrForbidden signals that the caller doesn't own the resource they're
// trying to mutate (e.g. cancelling someone else's ride).
var ErrForbidden = errors.New("forbidden")

// ErrConflict signals that the requested state change is invalid given the
// resource's current state (e.g. cancelling an already-terminal ride).
var ErrConflict = errors.New("conflict")
