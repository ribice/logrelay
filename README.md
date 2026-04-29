# logrelay

A Go library that reads structured JSON log lines from a stream and forwards error-level entries to Slack. Designed to sit between a process's stdout/stderr and your alerting pipeline — pipe your application logs through it and get Slack notifications for errors, panics, and fatal events.

## Features

- Parses structured JSON logs (supports `message` and `msg` fields for slog/zap/zerolog compatibility)
- Alerts on `error`, `fatal`, `panic`, and `dpanic` levels
- Deduplicates repeated errors within a configurable suppress window
- Retries failed Slack posts with exponential backoff and jitter
- Decodes base64-encoded stack traces
- Handles Dokku-style log prefixes (`timestamp app[process]:` before JSON)
- UTF-8-safe message truncation for Slack's character limits
- Context-aware — respects cancellation throughout
- Optional persistent log store (SQLite, pure-Go, no CGO) with a built-in HTML search UI

## Install

```bash
go get github.com/ribice/logrelay
```

## Usage

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/ribice/logrelay"
)

func main() {
	relay, err := logrelay.New(logrelay.Config{
		SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
		AppName:         "my-service",
		Source:          "dokku",
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := relay.Run(context.Background(), os.Stdin, os.Stderr); err != nil {
		log.Fatal(err)
	}
}
```

Pipe your application logs:

```bash
./my-service 2>&1 | logrelay
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `SlackWebhookURL` | *(required)* | Slack incoming webhook URL |
| `AppName` | `"application"` | Name shown in Slack messages |
| `Source` | `"logs"` | Source label (e.g. `"dokku"`, `"k8s"`) |
| `HTTPClient` | `http.DefaultClient` | HTTP client for Slack requests |
| `SuppressWindow` | `5m` | Duration to suppress duplicate errors; URLs, UUIDs, opaque tokens, long numbers, and durations are normalized so per-record failures do not spam Slack |
| `MaxRetries` | `3` | Max retry attempts for failed Slack posts |
| `InitialBackoff` | `500ms` | Initial backoff before first retry |
| `StorePath` | *(disabled)* | If set, every parsed log entry is written to a SQLite database at this path |
| `Retention` | `7d` | How long log rows are kept; cleanup runs on startup and opportunistically on insert |
| `BasicAuthUser` | *(disabled)* | If both `BasicAuthUser` and `BasicAuthPass` are set, `Handler()` requires HTTP Basic auth |
| `BasicAuthPass` | *(disabled)* | See `BasicAuthUser` |

## Search UI

When `StorePath` is set, every parsed log entry is written to a local SQLite database and `relay.Handler()` returns an `http.Handler` that serves:

- `GET /` — a self-contained, dependency-free HTML search UI (dark theme, level color-coding, live-search debounce, auto-refresh every 5s)
- `GET /api/logs` — a JSON API the UI calls (also useful for scripting / CLI consumers)

The UI is a single embedded HTML file (`ui.html`) — no JS bundles, no external assets. By default it serves without authentication; set `BasicAuthUser` and `BasicAuthPass` (see below) to gate it with HTTP Basic auth, or wrap `relay.Handler()` with your own middleware.

### Minimal integration

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"

    "github.com/ribice/logrelay"
)

func main() {
    relay, err := logrelay.New(logrelay.Config{
        SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
        AppName:         "my-service",
        StorePath:       "/var/lib/logrelay/logs.db",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer relay.Close()

    // Serve the UI on :8080 in the background.
    go func() {
        if err := http.ListenAndServe(":8080", relay.Handler()); err != nil {
            log.Printf("ui server: %v", err)
        }
    }()

    // Read logs from stdin and forward errors to Slack.
    if err := relay.Run(context.Background(), os.Stdin, os.Stderr); err != nil {
        log.Fatal(err)
    }
}
```

Pipe your application into it the same way as without the UI:

```bash
./my-service 2>&1 | logrelay
```

Then open `http://localhost:8080/`.

### Authentication

Set `BasicAuthUser` and `BasicAuthPass` on the `Config` and `Handler()` will gate every request with HTTP Basic auth. Comparisons are constant-time on hashed credentials:

```go
relay, _ := logrelay.New(logrelay.Config{
    SlackWebhookURL: os.Getenv("SLACK_WEBHOOK_URL"),
    StorePath:       "/var/lib/logrelay/logs.db",
    BasicAuthUser:   os.Getenv("LOGRELAY_USER"),
    BasicAuthPass:   os.Getenv("LOGRELAY_PASS"),
})
```

If you need something stronger (OAuth, SSO, mTLS), `relay.Handler()` is a plain `http.Handler`, so wrap it however you normally protect internal tooling:

```go
mux := http.NewServeMux()
mux.Handle("/logs/", http.StripPrefix("/logs", myAuthMiddleware(relay.Handler())))
http.ListenAndServe(":8080", mux)
```

### JSON API

`GET /api/logs` returns `{"logs": [...]}` ordered newest-first. Query parameters:

| Param | Type | Description |
|-------|------|-------------|
| `level` | string | Comma-separated list of levels (e.g. `error,warn`). Empty = all levels. |
| `q` | string | Case-insensitive substring search across `message`, `error_text`, `path`, and `request_id`. SQL `LIKE` wildcards (`%`, `_`) are escaped. |
| `limit` | int | Max rows to return (default 200, capped at 1000). |
| `before` | int | Cursor for pagination — returns rows with `id < before`. Use the `id` of the last row from the previous page. |

Each row contains: `id`, `ts` (unix nanos), `level`, `message`, plus optional `prefix`, `method`, `path`, `host`, `request_id`, `status_code`, `error_text`.

### Behavior notes

- The store records **every** parsed entry (all levels), not just the ones forwarded to Slack — so you can search through info/warn logs too.
- Old rows are deleted on startup and opportunistically on insert based on `Retention` (default 7 days).
- Without `StorePath`, `relay.Handler()` returns a 503 stub on every request — safe to wire up unconditionally.

## Log Format

logrelay expects JSON log lines with at least a `level` field. It recognizes these fields:

```json
{
  "level": "error",
  "time": "2026-04-11T12:00:00Z",
  "message": "panic recovered",
  "msg": "alternative message field",
  "method": "POST",
  "path": "/api/clubs",
  "host": "example.com",
  "request_id": "req-123",
  "status_code": 500,
  "error_message": "db timeout",
  "error": "connection refused",
  "err_message": "secondary error",
  "cause": "network failure",
  "stack": "base64-or-plaintext-stack-trace"
}
```

Lines prefixed with non-JSON text (e.g. Dokku timestamps) are handled automatically — the prefix is extracted and included in the Slack message.

## License

[MIT](LICENSE)
