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
| `SuppressWindow` | `5m` | Duration to suppress duplicate errors |
| `MaxRetries` | `3` | Max retry attempts for failed Slack posts |
| `InitialBackoff` | `500ms` | Initial backoff before first retry |

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
