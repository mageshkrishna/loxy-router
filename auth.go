package main

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// authMiddleware guards next with static bearer-token API keys.
//
// If keys is empty, auth is disabled and next is returned unwrapped — the
// scheduler runs open, matching its pre-Phase-3 behavior. When keys are
// configured, a request must carry "Authorization: Bearer <key>" matching one
// of them. Comparison is constant-time to avoid leaking key bytes via timing.
func authMiddleware(keys []string, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validKey(keys, bearerToken(r)) {
			writeJSONError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return h[len(prefix):]
	}
	return ""
}

func validKey(keys []string, token string) bool {
	if token == "" {
		return false
	}
	ok := false
	// Check every key (no early return) so timing does not reveal a prefix match.
	for _, k := range keys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(token)) == 1 {
			ok = true
		}
	}
	return ok
}
