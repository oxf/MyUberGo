package services

import "time"

// TokenIssuer hides JWT minting/parsing behind a port so application/command
// code depends on a capability, not golang-jwt directly.
type TokenIssuer interface {
	IssueAccess(userID, email, role string) (token string, expiresIn int, err error)
	IssueRefresh(userID string) (token string, expiresAt time.Time, err error)
	// ParseRefresh validates signature and claims, returning the token's
	// user_id. It does not check auth.refresh_token — that's the caller's job.
	ParseRefresh(token string) (userID string, err error)
}
