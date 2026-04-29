package logrelay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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

func TestStoreQueryByTimeRange(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()

	base := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	for i := range 5 {
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := s.Insert(ctx, ts, Entry{Level: "info", Message: "evt"}, ""); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Window [+1min, +4min) should match exactly 3 rows (i=1,2,3).
	rows, err := s.Query(ctx, queryParams{
		Since: base.Add(time.Minute).UnixNano(),
		Until: base.Add(4 * time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows in window, got %d", len(rows))
	}
}

func TestStoreQueryByStatusRange(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	statuses := []int{200, 302, 400, 404, 500, 503}
	for i, code := range statuses {
		if err := s.Insert(ctx, now.Add(time.Duration(i)*time.Second), Entry{Level: "info", Message: "evt", StatusCode: code}, ""); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	// Only 5xx.
	rows, err := s.Query(ctx, queryParams{StatusMin: 500, StatusMax: 599})
	if err != nil {
		t.Fatalf("Query 5xx: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 5xx rows, got %d", len(rows))
	}

	// 4xx and above.
	rows, err = s.Query(ctx, queryParams{StatusMin: 400})
	if err != nil {
		t.Fatalf("Query >=400: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows >=400, got %d", len(rows))
	}
}

func TestStoreDeleteByFilter(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	for i, e := range []Entry{
		{Level: "info", Message: "noisy"},
		{Level: "info", Message: "noisy"},
		{Level: "warn", Message: "slow"},
		{Level: "error", Message: "boom"},
	} {
		if err := s.Insert(ctx, now.Add(time.Duration(i)*time.Second), e, ""); err != nil {
			t.Fatalf("Insert(%d): %v", i, err)
		}
	}

	deleted, err := s.Delete(ctx, queryParams{Levels: []string{"info"}})
	if err != nil {
		t.Fatalf("Delete info: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}

	rows, err := s.Query(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 remaining, got %d", len(rows))
	}
}

func TestStoreDeleteAll(t *testing.T) {
	t.Parallel()
	s := newTestStore(t, 0)
	ctx := context.Background()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)

	for i := range 5 {
		if err := s.Insert(ctx, now.Add(time.Duration(i)*time.Second), Entry{Level: "info", Message: "x"}, ""); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	deleted, err := s.Delete(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Delete all: %v", err)
	}
	if deleted != 5 {
		t.Fatalf("expected 5 deleted, got %d", deleted)
	}

	rows, err := s.Query(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected store empty, got %d rows", len(rows))
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

func TestHandlerAPILogsTimeAndStatus(t *testing.T) {
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
	base := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		offset time.Duration
		entry  Entry
	}{
		{0, Entry{Level: "info", Message: "ok", StatusCode: 200}},
		{time.Minute, Entry{Level: "warn", Message: "slow", StatusCode: 304}},
		{2 * time.Minute, Entry{Level: "error", Message: "client error", StatusCode: 404}},
		{3 * time.Minute, Entry{Level: "error", Message: "boom", StatusCode: 500}},
	}
	for _, r := range rows {
		if err := relay.store.Insert(ctx, base.Add(r.offset), r.entry, ""); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	get := func(query string) []LogRow {
		t.Helper()
		res, err := http.Get(srv.URL + "/api/logs?" + query)
		if err != nil {
			t.Fatalf("GET %s: %v", query, err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("status = %d for %q", res.StatusCode, query)
		}
		var body struct {
			Logs []LogRow `json:"logs"`
		}
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body.Logs
	}

	// since= via unix nanos
	if hits := get("since=" + strconv.FormatInt(base.Add(2*time.Minute).UnixNano(), 10)); len(hits) != 2 {
		t.Fatalf("since=2min: expected 2, got %d", len(hits))
	}
	// until= via unix nanos
	if hits := get("until=" + strconv.FormatInt(base.Add(2*time.Minute).UnixNano(), 10)); len(hits) != 2 {
		t.Fatalf("until=2min: expected 2, got %d", len(hits))
	}
	// since= via RFC3339
	if hits := get("since=" + base.Add(time.Minute).Format(time.RFC3339Nano)); len(hits) != 3 {
		t.Fatalf("since RFC3339: expected 3, got %d", len(hits))
	}
	// status_min — only 4xx and 5xx
	if hits := get("status_min=400"); len(hits) != 2 {
		t.Fatalf("status_min=400: expected 2, got %d", len(hits))
	}
	// 5xx only
	if hits := get("status_min=500&status_max=599"); len(hits) != 1 {
		t.Fatalf("5xx: expected 1, got %d", len(hits))
	}
	// invalid since
	res, err := http.Get(srv.URL + "/api/logs?since=not-a-date")
	if err != nil {
		t.Fatalf("GET bad since: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad since, got %d", res.StatusCode)
	}
}

func TestHandlerAPILogsDelete(t *testing.T) {
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
		{Level: "info", Message: "x"},
		{Level: "info", Message: "y"},
		{Level: "error", Message: "boom"},
	} {
		if err := relay.store.Insert(ctx, now.Add(time.Duration(i)*time.Second), e, ""); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	srv := httptest.NewServer(relay.Handler())
	defer srv.Close()

	// Delete only info rows.
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/logs?level=info", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	var body struct {
		Deleted int64 `json:"deleted"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	res.Body.Close()
	if body.Deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", body.Deleted)
	}

	// One row remaining.
	rows, err := relay.store.Query(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 || rows[0].Level != "error" {
		t.Fatalf("expected 1 error row remaining, got %+v", rows)
	}

	// Delete all (no filters).
	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/api/logs", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE all: %v", err)
	}
	res.Body.Close()
	rows, err = relay.store.Query(ctx, queryParams{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected empty after DELETE all, got %d", len(rows))
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
