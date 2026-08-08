// Package httpresponse centralizes the JSON encode/decode/error-response
// idiom hand-copied across every service's HTTP handlers.
package httpresponse

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
)

// WriteJSON writes v as a JSON body with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes a plain-text error response.
func WriteError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

// WriteInternalError logs the real error server-side and returns a generic
// message, so internals (SQL errors, driver details, ...) never reach the
// HTTP response.
func WriteInternalError(w http.ResponseWriter, r *http.Request, err error, logger *logrus.Entry) {
	logger.WithContext(r.Context()).WithError(err).Error("internal error")
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

// Decode reads and JSON-decodes r.Body into a T.
func Decode[T any](r *http.Request) (T, error) {
	var v T
	err := json.NewDecoder(r.Body).Decode(&v)
	return v, err
}
