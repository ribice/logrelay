package logrelay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T, retention time.Duration) *store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "logs.db")
	s, err := openStore(path, retention)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStoreInsertAndQuery(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	entries := []Entry{
		{Level: "info", Message: "starting server"},
		{Level: "error", Message: "db failed", Path: "/clubs", StatusCode: 500, ErrorMessage: "timeout"},
		{Level: "warn", Message: "slow request", Path: "/health"},
		{Level: "error", Message: "panic recovered", RequestID: "req-xyz"},
	}
	for i, e := range entries {
		if err := s.Insert(ctx, now.Add(time.Duration(i)*time.Second), e, ""); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	all, err := s.Query(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Query all: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(all))
	}
	if all[0].Message != "panic recovered" { // ordered DESC by id
		t.Fatalf("expected newest first, got %q", all[0].Message)
	}

	errs, err := s.Query(ctx, queryParams{Levels: []string{"error"}})
	if err != nil {
		t.Fatalf("Query errors: %v", err)
	}
	if len(errs) != 2 {
		t.Fatalf("expected 2 error rows, got %d", len(errs))
	}

	hits, err := s.Query(ctx, queryParams{Query: "timeout"})
	if err != nil {
		t.Fatalf("Query timeout: %v", err)
	}
	if len(hits) != 1 || hits[0].ErrorText != "timeout" {
		t.Fatalf("expected one match on error_text, got %+v", hits)
	}

	byReq, err := s.Query(ctx, queryParams{Query: "req-xyz"})
	if err != nil {
		t.Fatalf("Query request_id: %v", err)
	}
	if len(byReq) != 1 {
		t.Fatalf("expected one match on request_id, got %d", len(byReq))
	}
}

func TestStoreRetentionCleanup(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, time.Hour)
	ctx := context.Background()
	old := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	// Insert one old entry, then enough recent entries to trigger cleanup.
	if err := s.Insert(ctx, old, Entry{Level: "error", Message: "ancient"}, ""); err != nil {
		t.Fatalf("insert old: %v", err)
	}

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	for i := range defaultCleanupEvery {
		if err := s.Insert(ctx, now, Entry{Level: "info", Message: "fresh"}, ""); err != nil {
			t.Fatalf("insert fresh %d: %v", i, err)
		}
	}

	rows, err := s.Query(ctx, queryParams{Query: "ancient"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected old entry to be cleaned up, got %d rows", len(rows))
	}
}

func TestStoreRetentionCleanupByTime(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, time.Hour)
	ctx := context.Background()
	old := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	if err := s.Insert(ctx, old, Entry{Level: "error", Message: "ancient"}, ""); err != nil {
		t.Fatalf("insert old: %v", err)
	}
	s.lastCleanup.Store(old.UnixNano())

	now := old.Add(2 * time.Hour)
	if err := s.Insert(ctx, now, Entry{Level: "info", Message: "fresh"}, ""); err != nil {
		t.Fatalf("insert fresh: %v", err)
	}

	rows, err := s.Query(ctx, queryParams{Query: "ancient"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected old entry to be cleaned up, got %d rows", len(rows))
	}
}

func TestStoreQueryEscapesLikeWildcards(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	entries := []Entry{
		{Level: "info", Message: "plain message"},
		{Level: "info", Message: "literal % marker"},
		{Level: "info", Message: "literal _ marker"},
	}
	for i, e := range entries {
		if err := s.Insert(ctx, now.Add(time.Duration(i)*time.Second), e, ""); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	percent, err := s.Query(ctx, queryParams{Query: "%"})
	if err != nil {
		t.Fatalf("Query percent: %v", err)
	}
	if len(percent) != 1 || percent[0].Message != "literal % marker" {
		t.Fatalf("expected one literal percent match, got %+v", percent)
	}

	underscore, err := s.Query(ctx, queryParams{Query: "_"})
	if err != nil {
		t.Fatalf("Query underscore: %v", err)
	}
	if len(underscore) != 1 || underscore[0].Message != "literal _ marker" {
		t.Fatalf("expected one literal underscore match, got %+v", underscore)
	}
}

func TestRunWritesToStore(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		StorePath:       filepath.Join(t.TempDir(), "logs.db"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer relay.Close()

	input := strings.NewReader(strings.Join([]string{
		`{"level":"info","message":"starting"}`,
		`{"level":"error","message":"boom","error":"db down"}`,
		`{"level":"warn","message":"slow"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows, err := relay.store.Query(context.Background(), queryParams{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 stored rows (all levels), got %d", len(rows))
	}
}

func TestHandlerAPILogs(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		StorePath:       filepath.Join(t.TempDir(), "logs.db"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer relay.Close()

	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	for i, e := range []Entry{
		{Level: "info", Message: "starting"},
		{Level: "error", Message: "db failed", ErrorMessage: "timeout"},
		{Level: "warn", Message: "slow request"},
	} {
		if err := relay.store.Insert(ctx, now.Add(time.Duration(i)*time.Second), e, ""); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	cases := []struct {
		name     string
		query    string
		wantHits int
	}{
		{name: "all", query: "", wantHits: 3},
		{name: "level filter", query: "level=error", wantHits: 1},
		{name: "substring on message", query: "q=slow", wantHits: 1},
		{name: "substring on error_text", query: "q=timeout", wantHits: 1},
		{name: "no match", query: "q=nothingmatcheshere", wantHits: 0},
		{name: "multi-level", query: "level=error,warn", wantHits: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := http.Get(srv.URL + "/api/logs?" + tc.query)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", res.StatusCode)
			}

			var body struct {
				Logs []LogRow `json:"logs"`
			}
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(body.Logs) != tc.wantHits {
				t.Fatalf("got %d hits, want %d (%+v)", len(body.Logs), tc.wantHits, body.Logs)
			}
		})
	}
}

func TestHandlerServesUI(t *testing.T) {
	t.Parallel()

	relay, err := New(Config{
		SlackWebhookURL: "https://hooks.slack.com/test",
		StorePath:       filepath.Join(t.TempDir(), "logs.db"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer relay.Close()

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "logrelay") {
		t.Fatalf("expected UI HTML, got %q", string(body[:min(200, len(body))]))
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("unexpected content-type %q", ct)
	}
}

func TestHandlerWithoutStore(t *testing.T) {
	t.Parallel()

	relay, err := New(Config{SlackWebhookURL: "https://hooks.slack.com/test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	res, err := http.Get(srv.URL + "/api/logs")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", res.StatusCode)
	}
}

func TestHandlerBasicAuth(t *testing.T) {
	t.Parallel()

	relay, err := New(Config{
		SlackWebhookURL: "https://hooks.slack.com/test",
		StorePath:       filepath.Join(t.TempDir(), "logs.db"),
		BasicAuthUser:   "admin",
		BasicAuthPass:   "s3cret",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer relay.Close()

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	cases := []struct {
		name       string
		path       string
		user, pass string
		setAuth    bool
		wantStatus int
		wantHeader bool
	}{
		{name: "no creds on UI", path: "/", setAuth: false, wantStatus: http.StatusUnauthorized, wantHeader: true},
		{name: "no creds on API", path: "/api/logs", setAuth: false, wantStatus: http.StatusUnauthorized, wantHeader: true},
		{name: "wrong user", path: "/", user: "nope", pass: "s3cret", setAuth: true, wantStatus: http.StatusUnauthorized, wantHeader: true},
		{name: "wrong pass", path: "/", user: "admin", pass: "wrong", setAuth: true, wantStatus: http.StatusUnauthorized, wantHeader: true},
		{name: "valid UI", path: "/", user: "admin", pass: "s3cret", setAuth: true, wantStatus: http.StatusOK},
		{name: "valid API", path: "/api/logs", user: "admin", pass: "s3cret", setAuth: true, wantStatus: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if tc.setAuth {
				req.SetBasicAuth(tc.user, tc.pass)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			defer res.Body.Close()
			if res.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if tc.wantHeader && !strings.HasPrefix(res.Header.Get("WWW-Authenticate"), "Basic") {
				t.Fatalf("expected WWW-Authenticate Basic header on 401, got %q", res.Header.Get("WWW-Authenticate"))
			}
		})
	}
}

func TestHandlerBasicAuthDisabledWhenEitherEmpty(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		user string
		pass string
	}{
		{name: "both empty", user: "", pass: ""},
		{name: "only user", user: "admin", pass: ""},
		{name: "only pass", user: "", pass: "s3cret"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			relay, err := New(Config{
				SlackWebhookURL: "https://hooks.slack.com/test",
				StorePath:       filepath.Join(t.TempDir(), "logs.db"),
				BasicAuthUser:   tc.user,
				BasicAuthPass:   tc.pass,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer relay.Close()

			srv := httptest.NewServer(relay.Handler())
			defer srv.Close()

			res, err := http.Get(srv.URL + "/api/logs")
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 (auth should be off)", res.StatusCode)
			}
		})
	}
}
