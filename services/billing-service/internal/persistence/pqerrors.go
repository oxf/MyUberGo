package persistence

import "github.com/lib/pq"

// isUniqueViolation reports whether err is a Postgres unique-constraint
// violation (SQLSTATE 23505) — used to convert an unavoidable INSERT race
// (the invoice idempotency guard, ledger-account get-or-create) into a
// distinguishable outcome instead of a raw driver error.
func isUniqueViolation(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == "23505"
}
