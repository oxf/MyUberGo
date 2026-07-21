package errors

import "errors"

// ErrNotFound signals that a lookup or update targeted a record that doesn't
// exist. Repositories return it instead of (nil, nil) / a silently-ignored
// zero-rows-affected update, so "nothing happened" is a distinguishable,
// checkable (errors.Is) outcome rather than indistinguishable from success.
var ErrNotFound = errors.New("not found")

// ErrOfferGone: the offer expired, the ride was cancelled, or the ride was
// never offered to this driver. HTTP layer maps it to 400.
var ErrOfferGone = errors.New("offer expired, cancelled, or not offered to this driver")

// ErrRideTaken: another driver won the atomic accept race. HTTP maps it to 409.
var ErrRideTaken = errors.New("ride already accepted by another driver")
