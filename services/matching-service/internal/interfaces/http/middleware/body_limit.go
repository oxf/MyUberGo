package middleware

import "net/http"

const maxRequestBodyBytes = 1 << 20 // 1MB

// BodyLimit caps the size of the incoming request body so a handler's
// json.Decode can't be used to exhaust server memory with an unbounded body.
func BodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}
