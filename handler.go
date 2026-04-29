package logrelay

import (
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

//go:embed ui.html
var uiHTML []byte

// Handler returns an http.Handler serving the search UI at "/" and a
// JSON API at "/api/logs". When the relay was created without a
// StorePath, it returns 503 for every request.
//
// If Config.BasicAuthUser and BasicAuthPass were set, every request is
// gated by HTTP Basic auth. Otherwise the handler is unauthenticated —
// mount it behind your own auth middleware if exposing publicly.
func (r *Relay) Handler() http.Handler {
	mux := http.NewServeMux()

	if r.store == nil {
		mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "logrelay store is not enabled (set Config.StorePath)", http.StatusServiceUnavailable)
		})
	} else {
		mux.HandleFunc("GET /api/logs", r.handleAPILogs)
		mux.HandleFunc("GET /", func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path != "/" {
				http.NotFound(w, req)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			_, _ = w.Write(uiHTML)
		})
	}

	if r.basicAuthUser != "" && r.basicAuthPass != "" {
		return basicAuth(mux, r.basicAuthUser, r.basicAuthPass)
	}
	return mux
}

func basicAuth(next http.Handler, user, pass string) http.Handler {
	// Hash credentials once so the per-request comparison is constant-time
	// regardless of length, and avoids leaking the configured values via
	// timing on early-mismatch byte comparisons.
	wantUser := sha256.Sum256([]byte(user))
	wantPass := sha256.Sum256([]byte(pass))
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotUser, gotPass, ok := req.BasicAuth()
		if ok {
			gotUserHash := sha256.Sum256([]byte(gotUser))
			gotPassHash := sha256.Sum256([]byte(gotPass))
			if subtle.ConstantTimeCompare(gotUserHash[:], wantUser[:]) == 1 &&
				subtle.ConstantTimeCompare(gotPassHash[:], wantPass[:]) == 1 {
				next.ServeHTTP(w, req)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="logrelay", charset="UTF-8"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func (r *Relay) handleAPILogs(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()

	params := queryParams{
		Query: q.Get("q"),
	}

	if lvl := strings.TrimSpace(q.Get("level")); lvl != "" {
		for p := range strings.SplitSeq(lvl, ",") {
			if s := strings.TrimSpace(p); s != "" {
				params.Levels = append(params.Levels, s)
			}
		}
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			params.Limit = n
		}
	}
	if v := q.Get("before"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			params.Before = n
		}
	}

	logs, err := r.store.Query(req.Context(), params)
	if err != nil {
		http.Error(w, "query failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"logs": logs})
}
