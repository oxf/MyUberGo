package errors

import "errors"

// ErrNotFound signals that a lookup or update targeted a record that doesn't
// exist. Repositories return it instead of (nil, nil) / a silently-ignored
// zero-rows-affected update, so "nothing happened" is a distinguishable,
// checkable (errors.Is) outcome rather than indistinguishable from success.
var ErrNotFound = errors.New("not found")

// ErrInvalidCredentials signals a login attempt with an unknown email or a
// wrong password. Both cases map to the same error deliberately, so the
// handler can't be used to enumerate registered emails.
var ErrInvalidCredentials = errors.New("invalid credentials")

// ErrInvalidToken signals a refresh token that doesn't parse, isn't found in
// auth.refresh_token, or has expired/been revoked.
var ErrInvalidToken = errors.New("invalid token")

// ErrConflict signals a signup with an email that's already registered.
var ErrConflict = errors.New("email already registered")
