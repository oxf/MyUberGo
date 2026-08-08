// Package kongheaders reads the caller-identity headers Kong's jwt +
// inject_user_headers plugins set on every proxied request (see
// gateway/kong.yml). Downstream services trust these headers rather than
// validating the JWT themselves, since Kong is the sole ingress.
package kongheaders

import (
	"net/http"

	"github.com/oxf/MyUber/common/httpresponse"
)

const (
	HeaderUserID   = "X-User-Id"
	HeaderClientID = "X-Client-Id"
)

// UserID reads X-User-Id, ok is false when it's missing/empty.
func UserID(r *http.Request) (string, bool) {
	v := r.Header.Get(HeaderUserID)
	return v, v != ""
}

// ClientID reads X-Client-Id, ok is false when it's missing/empty.
func ClientID(r *http.Request) (string, bool) {
	v := r.Header.Get(HeaderClientID)
	return v, v != ""
}

// RequireUserID reads X-User-Id, writing a 400 and returning ok=false if missing.
func RequireUserID(w http.ResponseWriter, r *http.Request) (id string, ok bool) {
	id, ok = UserID(r)
	if !ok {
		httpresponse.WriteError(w, "X-User-Id header is required", http.StatusBadRequest)
	}
	return id, ok
}

// RequireClientID reads X-Client-Id, writing a 400 and returning ok=false if missing.
func RequireClientID(w http.ResponseWriter, r *http.Request) (id string, ok bool) {
	id, ok = ClientID(r)
	if !ok {
		httpresponse.WriteError(w, "X-Client-Id header is required", http.StatusBadRequest)
	}
	return id, ok
}
