package logrelay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultScannerBufferSize = 64 * 1024
	maxScannerBufferSize     = 1024 * 1024
	maxSlackMessageLen       = 3500
	maxStackLen              = 1200
	defaultSuppressWindow    = 5 * time.Minute
	defaultMaxRetries        = 3
	defaultInitialBackoff    = 500 * time.Millisecond
	defaultAppName           = "application"
	defaultSource            = "logs"
	maxLastSentEntries       = 10000
)

type Config struct {
	SlackWebhookURL string
	AppName         string
	Source          string
	HTTPClient      *http.Client
	SuppressWindow  time.Duration
	MaxRetries      int
	InitialBackoff  time.Duration
}

type Relay struct {
	slackWebhookURL string
	appName         string
	source          string
	httpClient      *http.Client
	suppressWindow  time.Duration
	maxRetries      int
	initialBackoff  time.Duration
	now             func() time.Time
	mu              sync.Mutex
	lastSent        map[string]time.Time
}

type Entry struct {
	Prefix       string `json:"-"`
	Level        string `json:"level"`
	Time         string `json:"time"`
	Message      string `json:"message"`
	Msg          string `json:"msg"` // fallback for Message (used by slog/zap)
	Method       string `json:"method"`
	Path         string `json:"path"`
	Host         string `json:"host"`
	RequestID    string `json:"request_id"`
	StatusCode   int    `json:"status_code"`
	ErrorMessage string `json:"error_message"`
	Err          string `json:"error"`
	ErrMessage   string `json:"err_message"`
	Cause        string `json:"cause"`
	Stack        string `json:"stack"`
}

func New(cfg Config) (*Relay, error) {
	if strings.TrimSpace(cfg.SlackWebhookURL) == "" {
		return nil, errors.New("slack webhook URL is required")
	}

	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	appName := strings.TrimSpace(cfg.AppName)
	if appName == "" {
		appName = defaultAppName
	}

	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = defaultSource
	}

	suppressWindow := cfg.SuppressWindow
	if suppressWindow <= 0 {
		suppressWindow = defaultSuppressWindow
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	initialBackoff := cfg.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = defaultInitialBackoff
	}

	return &Relay{
		slackWebhookURL: cfg.SlackWebhookURL,
		appName:         appName,
		source:          source,
		httpClient:      client,
		suppressWindow:  suppressWindow,
		maxRetries:      maxRetries,
		initialBackoff:  initialBackoff,
		now:             time.Now,
		lastSent:        make(map[string]time.Time),
	}, nil
}

func (r *Relay) Run(ctx context.Context, input io.Reader, stderr io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, defaultScannerBufferSize), maxScannerBufferSize)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		entry, ok := parseEntry(line)
		if !ok {
			continue
		}

		if !shouldAlert(entry.Level) {
			continue
		}

		if r.shouldSuppress(entry) {
			continue
		}

		if err := r.postToSlack(ctx, formatSlackMessage(r.appName, r.source, entry)); err != nil {
			if stderr != nil {
				_, _ = fmt.Fprintf(stderr, "logrelay: post to slack failed: %v\n", err)
			}
			continue
		}

		r.markSent(entry)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan log stream: %w", err)
	}

	return nil
}

func parseEntry(line string) (Entry, bool) {
	var entry Entry
	jsonLine := line
	if idx := strings.IndexByte(line, '{'); idx > 0 {
		entry.Prefix = strings.TrimSuffix(strings.TrimSpace(line[:idx]), ":")
		jsonLine = line[idx:]
	}
	if err := json.Unmarshal([]byte(jsonLine), &entry); err != nil {
		return Entry{}, false
	}
	if strings.TrimSpace(entry.Level) == "" {
		return Entry{}, false
	}
	if entry.Message == "" {
		entry.Message = entry.Msg
	}
	return entry, true
}

func shouldAlert(level string) bool {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "error", "fatal", "panic", "dpanic":
		return true
	default:
		return false
	}
}

func formatSlackMessage(appName, source string, entry Entry) string {
	lines := []string{
		fmt.Sprintf(":rotating_light: *%s* `%s`", appName, strings.ToLower(entry.Level)),
	}

	if source != "" {
		lines = append(lines, fmt.Sprintf("Source: `%s`", source))
	}
	if entry.Prefix != "" {
		lines = append(lines, fmt.Sprintf("Log Prefix: `%s`", entry.Prefix))
	}
	if entry.Time != "" {
		lines = append(lines, fmt.Sprintf("Time: `%s`", entry.Time))
	}
	if entry.Message != "" {
		lines = append(lines, fmt.Sprintf("Message: %s", entry.Message))
	}

	requestSummary := strings.TrimSpace(strings.Join([]string{entry.Method, entry.Path}, " "))
	if requestSummary != "" || entry.StatusCode > 0 {
		if entry.StatusCode > 0 {
			requestSummary = strings.TrimSpace(fmt.Sprintf("%s -> %d", requestSummary, entry.StatusCode))
		}
		lines = append(lines, fmt.Sprintf("Request: `%s`", requestSummary))
	}

	if entry.Host != "" {
		lines = append(lines, fmt.Sprintf("Host: `%s`", entry.Host))
	}
	if entry.RequestID != "" {
		lines = append(lines, fmt.Sprintf("Request ID: `%s`", entry.RequestID))
	}

	errorDetails := make([]string, 0, 3)
	if entry.ErrorMessage != "" {
		errorDetails = append(errorDetails, entry.ErrorMessage)
	}
	if entry.ErrMessage != "" && entry.ErrMessage != entry.ErrorMessage {
		errorDetails = append(errorDetails, entry.ErrMessage)
	}
	if entry.Err != "" {
		errorDetails = append(errorDetails, entry.Err)
	}
	if entry.Cause != "" {
		errorDetails = append(errorDetails, fmt.Sprintf("cause: %s", entry.Cause))
	}
	if len(errorDetails) > 0 {
		lines = append(lines, fmt.Sprintf("Details: %s", strings.Join(errorDetails, " | ")))
	}

	if stack := decodeStack(entry.Stack); stack != "" {
		stack = truncate(stack, maxStackLen)
		lines = append(lines, fmt.Sprintf("Stack:\n```%s```", stack))
	}

	return truncate(strings.Join(lines, "\n"), maxSlackMessageLen)
}

func decodeStack(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err == nil && utf8.Valid(decoded) {
		return string(decoded)
	}
	return raw
}

func truncate(value string, maxLen int) string {
	if len(value) <= maxLen {
		return value
	}
	// Walk runes to find the last complete rune that fits within maxLen bytes.
	truncated := 0
	for i := range value {
		if i > maxLen {
			break
		}
		truncated = i
	}
	return value[:truncated] + "...(truncated)"
}

func (r *Relay) postToSlack(ctx context.Context, msg string) error {
	body, err := json.Marshal(map[string]string{"text": msg})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	backoff := r.initialBackoff
	var lastErr error

	for attempt := 1; attempt <= r.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.slackWebhookURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("send request: %w", err)
		} else if resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		} else {
			payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(payload)))
		}

		if attempt == r.maxRetries {
			break
		}
		// Add jitter: sleep between 50%-100% of backoff to avoid thundering herd.
		jittered := backoff/2 + time.Duration(rand.Int64N(int64(backoff/2+1)))
		if err := sleepWithContext(ctx, jittered); err != nil {
			return errors.Join(lastErr, err)
		}
		backoff *= 2
	}

	return lastErr
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Relay) shouldSuppress(entry Entry) bool {
	key := dedupeKey(entry)
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	lastSeen, ok := r.lastSent[key]
	return ok && now.Sub(lastSeen) < r.suppressWindow
}

func (r *Relay) markSent(entry Entry) {
	key := dedupeKey(entry)
	now := r.now()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastSent[key] = now

	if len(r.lastSent) < maxLastSentEntries/10 {
		return
	}

	cutoff := now.Add(-r.suppressWindow * 2)
	for k, ts := range r.lastSent {
		if ts.Before(cutoff) {
			delete(r.lastSent, k)
		}
	}

	for len(r.lastSent) > maxLastSentEntries {
		var oldestKey string
		var oldestTime time.Time
		for k, ts := range r.lastSent {
			if oldestKey == "" || ts.Before(oldestTime) {
				oldestKey = k
				oldestTime = ts
			}
		}
		delete(r.lastSent, oldestKey)
	}
}

func dedupeKey(entry Entry) string {
	parts := []string{
		strings.ToLower(strings.TrimSpace(entry.Level)),
		strings.TrimSpace(entry.Message),
		strings.TrimSpace(entry.Method),
		strings.TrimSpace(entry.Path),
		fmt.Sprintf("%d", entry.StatusCode),
		strings.TrimSpace(entry.ErrorMessage),
		strings.TrimSpace(entry.Err),
		strings.TrimSpace(entry.ErrMessage),
		strings.TrimSpace(entry.Cause),
		strings.TrimSpace(entry.Prefix),
	}
	return strings.Join(parts, "|")
}
