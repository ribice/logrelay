package logrelay

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestShouldAlert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		level string
		want  bool
	}{
		{level: "error", want: true},
		{level: "fatal", want: true},
		{level: "panic", want: true},
		{level: "dpanic", want: true},
		{level: "warn", want: false},
		{level: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			t.Parallel()
			if got := shouldAlert(tt.level); got != tt.want {
				t.Fatalf("shouldAlert(%q) = %v, want %v", tt.level, got, tt.want)
			}
		})
	}
}

func TestFormatSlackMessageDecodesBase64Stack(t *testing.T) {
	t.Parallel()

	stack := base64.StdEncoding.EncodeToString([]byte("panic line 1\npanic line 2"))
	msg := formatSlackMessage("my-service", "dokku", Entry{
		Level:      "error",
		Message:    "panic recovered",
		Method:     "GET",
		Path:       "/health",
		StatusCode: http.StatusInternalServerError,
		Stack:      stack,
	})

	if !strings.Contains(msg, "panic recovered") {
		t.Fatalf("expected message in slack payload, got %q", msg)
	}
	if !strings.Contains(msg, "panic line 1") {
		t.Fatalf("expected decoded stack in slack payload, got %q", msg)
	}
}

func TestParseEntryStripsDokkuPrefix(t *testing.T) {
	t.Parallel()

	entry, _, ok := parseEntry(`2026-04-11T12:00:00Z app[web.1]: {"level":"error","message":"panic recovered","path":"/clubs"}`)
	if !ok {
		t.Fatal("expected parseEntry to succeed")
	}
	if entry.Prefix != "2026-04-11T12:00:00Z app[web.1]" {
		t.Fatalf("unexpected prefix %q", entry.Prefix)
	}
	if entry.Path != "/clubs" {
		t.Fatalf("unexpected path %q", entry.Path)
	}
}

func TestRunPostsOnlyErrorEntries(t *testing.T) {
	t.Parallel()

	var posted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		posted = append(posted, payload["text"])
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	input := strings.NewReader(strings.Join([]string{
		`{"level":"info","message":"starting server"}`,
		`{"level":"error","message":"panic recovered","method":"POST","path":"/clubs","status_code":500,"error_message":"db timeout"}`,
		`this is not json`,
		`{"level":"warn","message":"slow request"}`,
		`{"level":"fatal","message":"server error","error":"listen tcp :8080: bind: address already in use"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(posted) != 2 {
		t.Fatalf("expected 2 slack posts, got %d", len(posted))
	}
	if !strings.Contains(posted[0], "db timeout") {
		t.Fatalf("expected request error details in first payload, got %q", posted[0])
	}
	if !strings.Contains(posted[1], "address already in use") {
		t.Fatalf("expected fatal error details in second payload, got %q", posted[1])
	}
}

func TestRunSuppressesDuplicateAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"db failed","path":"/clubs","status_code":500}`,
		`{"level":"error","message":"db failed","path":"/clubs","status_code":500}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post, got %d", got)
	}
}

func TestRunSuppressesIDVariantAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"[repair] pravosudje vijest id=146808: pravosudje vijest API ne vraća validan odgovor"}`,
		`{"level":"error","message":"[repair] pravosudje vijest id=146803: pravosudje vijest API ne vraća validan odgovor"}`,
		`{"level":"error","message":"[repair] pravosudje vijest id=146807: pravosudje vijest API ne vraća validan odgovor"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for id variants, got %d", got)
	}
}

func TestRunSuppressesLongNumberVariantAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"[repair] pravosudje vijest article 146808: API ne vraća validan odgovor"}`,
		`{"level":"error","message":"[repair] pravosudje vijest article 146803: API ne vraća validan odgovor"}`,
		`{"level":"error","message":"[repair] pravosudje vijest article 146807: API ne vraća validan odgovor"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for long number variants, got %d", got)
	}
}

func TestRunSuppressesUUIDVariantAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"job 018f9c2e-7b8d-7e0a-a4e8-c0f83b7fd111 failed: API ne vraća validan odgovor"}`,
		`{"level":"error","message":"job 018f9c2e-7b8d-7e0a-a4e8-c0f83b7fd222 failed: API ne vraća validan odgovor"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for UUID variants, got %d", got)
	}
}

func TestRunSuppressesOpaqueSourceVariantAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"[runonce] create child run failed for source=d7hs2332452s70gltabg: insert scrape run: context canceled"}`,
		`{"level":"error","message":"[runonce] create child run failed for source=d7hsk3b2452s70gltakg: insert scrape run: context canceled"}`,
		`{"level":"error","message":"[runonce] create child run failed for source=d7hsm6j2452s70gltapg: insert scrape run: context canceled"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for opaque source variants, got %d", got)
	}
}

func TestRunSuppressesURLVariantAlertsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"[doc] https://www.mupzzh.ba/node/436 - direct attachment fetch failed url=https://www.mupzzh.ba/sites/default/files/javne-nabavke/Usluge%20osiguranja.pdf err=status 404"}`,
		`{"level":"error","message":"[doc] https://www.mupzzh.ba/node/435 - direct attachment fetch failed url=https://www.mupzzh.ba/sites/default/files/javne-nabavke/odluka%20o%20izboru.pdf err=status 404"}`,
		`{"level":"error","message":"[doc] https://www.mupzzh.ba/node/417 - direct attachment fetch failed url=https://www.mupzzh.ba/sites/default/files/javne-nabavke/Odluka%20tehnicki%20pregled.pdf err=status 404"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for URL variants, got %d", got)
	}
}

func TestRunSuppressesPathTokenAndDurationVariantsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`{"level":"error","message":"/scrape-sources/d7f3n4qnh4rs70u85230 500 13ms"}`,
		`{"level":"error","message":"/scrape-sources/d7f3n4qnh4rs70u85231 500 27ms"}`,
		`{"level":"error","message":"/scrape-sources/d7f3n4qnh4rs70u85232 500 91ms"}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for path token and duration variants, got %d", got)
	}
}

func TestRunSuppressesDokkuPrefixVariantsWithinWindow(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	input := strings.NewReader(strings.Join([]string{
		`2026-04-11T12:00:00Z app[web.1]: {"level":"error","message":"db failed","path":"/clubs","status_code":500}`,
		`2026-04-11T12:00:01Z app[web.1]: {"level":"error","message":"db failed","path":"/clubs","status_code":500}`,
		`2026-04-11T12:00:02.123456Z app[web.1]: {"level":"error","message":"db failed","path":"/clubs","status_code":500}`,
	}, "\n"))

	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 slack post for dokku prefix variants, got %d", got)
	}
}

func TestRunRetriesSlackPost(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call < 3 {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		MaxRetries:      3,
		InitialBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var stderr bytes.Buffer
	input := strings.NewReader(`{"level":"error","message":"server error","error":"connection reset"}`)

	if err := relay.Run(context.Background(), input, &stderr); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 slack attempts, got %d", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestTruncateUTF8Safety(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		max   int
	}{
		{name: "ascii within limit", input: "hello", max: 10},
		{name: "ascii truncated", input: "hello world", max: 5},
		{name: "multibyte intact", input: "café", max: 10},
		{name: "multibyte truncated mid-rune", input: "café", max: 4}, // 'é' is 2 bytes at position 3-4
		{name: "emoji truncated", input: "hi 😀 there", max: 5},        // emoji is 4 bytes
		{name: "cjk truncated", input: "日本語テスト", max: 7},              // each char is 3 bytes
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := truncate(tt.input, tt.max)
			if !utf8.ValidString(got) {
				t.Fatalf("truncate(%q, %d) produced invalid UTF-8: %q", tt.input, tt.max, got)
			}
		})
	}
}

func TestTruncatePreservesShortStrings(t *testing.T) {
	t.Parallel()

	input := "short"
	got := truncate(input, 100)
	if got != input {
		t.Fatalf("truncate should return input unchanged, got %q", got)
	}
}

func TestPostToSlackRetriesExhausted(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "always fails", http.StatusBadGateway)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		MaxRetries:      3,
		InitialBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var stderr bytes.Buffer
	input := strings.NewReader(`{"level":"error","message":"fail","error":"boom"}`)

	if err := relay.Run(context.Background(), input, &stderr); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := calls.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
	if !strings.Contains(stderr.String(), "post to slack failed") {
		t.Fatalf("expected failure in stderr, got %q", stderr.String())
	}
}

func TestRunContextCancellation(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		InitialBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// With a canceled context, postToSlack fails immediately. The error line
	// still gets processed but the HTTP request returns context.Canceled.
	input := strings.NewReader(`{"level":"error","message":"test"}`)
	var stderr bytes.Buffer
	if err := relay.Run(ctx, input, &stderr); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !strings.Contains(stderr.String(), "post to slack failed") {
		t.Fatalf("expected slack failure in stderr, got %q", stderr.String())
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "empty URL", cfg: Config{}, wantErr: true},
		{name: "whitespace URL", cfg: Config{SlackWebhookURL: "  "}, wantErr: true},
		{name: "valid URL", cfg: Config{SlackWebhookURL: "https://hooks.slack.com/test"}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()

	relay, err := New(Config{SlackWebhookURL: "https://hooks.slack.com/test"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if relay.appName != "application" {
		t.Fatalf("expected default appName, got %q", relay.appName)
	}
	if relay.source != "logs" {
		t.Fatalf("expected default source, got %q", relay.source)
	}
	if relay.suppressWindow != 5*time.Minute {
		t.Fatalf("expected default suppressWindow, got %v", relay.suppressWindow)
	}
	if relay.maxRetries != 3 {
		t.Fatalf("expected default maxRetries, got %d", relay.maxRetries)
	}
	if relay.initialBackoff != 500*time.Millisecond {
		t.Fatalf("expected default initialBackoff, got %v", relay.initialBackoff)
	}
}

func TestMarkSentCleansUpExpiredEntries(t *testing.T) {
	t.Parallel()

	relay, err := New(Config{
		SlackWebhookURL: "https://hooks.slack.com/test",
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	// Fill past the cleanup threshold (maxLastSentEntries/10 = 1000).
	for i := range maxLastSentEntries/10 + 1 {
		relay.markSent(Entry{Level: "error", Message: fmt.Sprintf("msg-%d", i)})
	}

	before := len(relay.lastSent)
	if before <= maxLastSentEntries/10 {
		t.Fatalf("expected more than %d entries, got %d", maxLastSentEntries/10, before)
	}

	// Advance time past 2x suppress window and send another entry to trigger cleanup.
	now = now.Add(3 * time.Minute)
	relay.markSent(Entry{Level: "error", Message: "trigger-cleanup"})

	// All old entries should have been cleaned up, leaving only the new one.
	if len(relay.lastSent) != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", len(relay.lastSent))
	}
}

func TestSuppressWindowExpiry(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	relay, err := New(Config{
		SlackWebhookURL: server.URL,
		HTTPClient:      server.Client(),
		SuppressWindow:  time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)
	relay.now = func() time.Time { return now }

	// First occurrence: should post.
	line := `{"level":"error","message":"db failed","path":"/clubs","status_code":500}`
	input := strings.NewReader(line)
	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Same error within window: should suppress.
	input = strings.NewReader(line)
	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 call within window, got %d", got)
	}

	// Advance past suppress window: should post again.
	now = now.Add(2 * time.Minute)
	input = strings.NewReader(line)
	if err := relay.Run(context.Background(), input, io.Discard); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 calls after window expiry, got %d", got)
	}
}

func TestFormatSlackMessageAllFields(t *testing.T) {
	t.Parallel()

	entry := Entry{
		Prefix:       "app[web.1]",
		Level:        "error",
		Time:         "2026-04-11T12:00:00Z",
		Message:      "panic recovered",
		Method:       "POST",
		Path:         "/api/clubs",
		Host:         "example.com",
		RequestID:    "req-123",
		StatusCode:   500,
		ErrorMessage: "db timeout",
		Err:          "connection refused",
		ErrMessage:   "secondary error",
		Cause:        "network failure",
	}

	msg := formatSlackMessage("my-app", "dokku", entry)

	for _, want := range []string{
		"my-app", "error", "dokku", "app[web.1]",
		"2026-04-11T12:00:00Z", "panic recovered",
		"POST", "/api/clubs", "500", "example.com",
		"req-123", "db timeout", "connection refused",
		"secondary error", "network failure",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected %q in message, got:\n%s", want, msg)
		}
	}
}

func TestParseEntryEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line string
		ok   bool
	}{
		{name: "empty", line: "", ok: false},
		{name: "whitespace", line: "   ", ok: false},
		{name: "not json", line: "this is plain text", ok: false},
		{name: "json without level", line: `{"message":"hello"}`, ok: false},
		{name: "json with empty level", line: `{"level":"","message":"hello"}`, ok: false},
		{name: "valid json", line: `{"level":"error","message":"boom"}`, ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, got := parseEntry(tt.line)
			if got != tt.ok {
				t.Fatalf("parseEntry(%q) ok = %v, want %v", tt.line, got, tt.ok)
			}
		})
	}
}

func TestParseEntryMsgFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		line    string
		wantMsg string
	}{
		{
			name:    "msg field used when message absent",
			line:    `{"level":"error","msg":"slog style"}`,
			wantMsg: "slog style",
		},
		{
			name:    "message takes priority over msg",
			line:    `{"level":"error","message":"primary","msg":"fallback"}`,
			wantMsg: "primary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			entry, _, ok := parseEntry(tt.line)
			if !ok {
				t.Fatal("expected parseEntry to succeed")
			}
			if entry.Message != tt.wantMsg {
				t.Fatalf("got Message=%q, want %q", entry.Message, tt.wantMsg)
			}
		})
	}
}

func TestDecodeStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "", want: ""},
		{name: "whitespace", raw: "   ", want: ""},
		{name: "valid base64", raw: base64.StdEncoding.EncodeToString([]byte("goroutine 1")), want: "goroutine 1"},
		{name: "not base64", raw: "plain stack trace", want: "plain stack trace"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := decodeStack(tt.raw)
			if got != tt.want {
				t.Fatalf("decodeStack(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
