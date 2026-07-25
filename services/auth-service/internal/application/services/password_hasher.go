package services

// PasswordHasher hides the concrete hashing algorithm (bcrypt today) behind
// a port so application/command code depends on a capability, not a library.
type PasswordHasher interface {
	Hash(plain string) (string, error)
	// Compare returns a non-nil error when plain does not match hash.
	Compare(hash, plain string) error
}
