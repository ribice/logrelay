package logrelay

import (
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
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
		mux.HandleFunc("DELETE /api/logs", r.handleAPILogsDelete)
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
	params, err := parseQueryParams(req.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

// handleAPILogsDelete removes rows matching the same filter set as GET. With
// no filters, the entire store is wiped — callers should gate this behind
// auth and ideally a UI confirmation.
func (r *Relay) handleAPILogsDelete(w http.ResponseWriter, req *http.Request) {
	params, err := parseQueryParams(req.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	deleted, err := r.store.Delete(req.Context(), params)
	if err != nil {
		http.Error(w, "delete failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"deleted": deleted})
}

func parseQueryParams(q url.Values) (queryParams, error) {
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
	if v := strings.TrimSpace(q.Get("since")); v != "" {
		ns, err := parseTimeParam(v)
		if err != nil {
			return queryParams{}, err
		}
		params.Since = ns
	}
	if v := strings.TrimSpace(q.Get("until")); v != "" {
		ns, err := parseTimeParam(v)
		if err != nil {
			return queryParams{}, err
		}
		params.Until = ns
	}
	if v := q.Get("status_min"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			params.StatusMin = n
		}
	}
	if v := q.Get("status_max"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			params.StatusMax = n
		}
	}

	return params, nil
}

// parseTimeParam accepts unix-nanos as integer or RFC3339 timestamp and
// returns unix nanos. Empty input is rejected — callers should check for
// empty before calling.
func parseTimeParam(v string) (int64, error) {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UnixNano(), nil
	}
	return 0, &timeParamError{value: v}
}

type timeParamError struct{ value string }

func (e *timeParamError) Error() string {
	return "invalid time value " + strconv.Quote(e.value) + ": expected unix nanos or RFC3339"
}
