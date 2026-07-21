package errors

import "errors"

// ErrNotFound signals that a lookup or update targeted a record that doesn't
// exist. Repositories return it instead of (nil, nil) / a silently-ignored
// zero-rows-affected update, so "nothing happened" is a distinguishable,
// checkable (errors.Is) outcome rather than indistinguishable from success.
var ErrNotFound = errors.New("not found")
