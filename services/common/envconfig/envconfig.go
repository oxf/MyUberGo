// Package envconfig centralizes the getenv-with-default idiom every service's
// cmd/main.go hand-rolled independently.
package envconfig

import (
	"os"
	"strconv"
)

// String returns the value of the environment variable key, or def if unset/empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Int returns the parsed integer value of the environment variable key, or def
// if unset/empty. A value that fails to parse yields 0, not def — this matches
// every service's existing `atoi(getenv(...))` behavior exactly.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, _ := strconv.Atoi(v)
	return n
}
