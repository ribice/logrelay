package logrelay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultRetention     = 7 * 24 * time.Hour
	defaultCleanupEvery  = 1000
	defaultCleanupPeriod = time.Hour
	storeMaxQueryLimit   = 1000
	storeDefaultQueryLim = 200
)

type store struct {
	db          *sql.DB
	retention   time.Duration
	insertCount atomic.Uint64
	lastCleanup atomic.Int64
}

func openStore(path string, retention time.Duration) (*store, error) {
	if retention <= 0 {
		retention = defaultRetention
	}

	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // single writer; readers go through the same conn fine

	schema := `
CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    level       TEXT    NOT NULL,
    message     TEXT    NOT NULL DEFAULT '',
    prefix      TEXT    NOT NULL DEFAULT '',
    method      TEXT    NOT NULL DEFAULT '',
    path        TEXT    NOT NULL DEFAULT '',
    host        TEXT    NOT NULL DEFAULT '',
    request_id  TEXT    NOT NULL DEFAULT '',
    status_code INTEGER NOT NULL DEFAULT 0,
    error_text  TEXT    NOT NULL DEFAULT '',
    raw         TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_logs_ts    ON logs(ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level, ts DESC);
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	s := &store{db: db, retention: retention}
	now := time.Now()
	if err := s.cleanup(context.Background(), now); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cleanup old logs: %w", err)
	}
	s.lastCleanup.Store(now.UnixNano())
	return s, nil
}

func (s *store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *store) Insert(ctx context.Context, ts time.Time, entry Entry, raw string) error {
	errorText := strings.TrimSpace(strings.Join(filterEmpty([]string{
		entry.ErrorMessage, entry.ErrMessage, entry.Err, entry.Cause,
	}), " | "))

	_, err := s.db.ExecContext(ctx, `
INSERT INTO logs (ts, level, message, prefix, method, path, host, request_id, status_code, error_text, raw)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ts.UnixNano(),
		strings.ToLower(strings.TrimSpace(entry.Level)),
		entry.Message,
		entry.Prefix,
		entry.Method,
		entry.Path,
		entry.Host,
		entry.RequestID,
		entry.StatusCode,
		errorText,
		raw,
	)
	if err != nil {
		return fmt.Errorf("insert log: %w", err)
	}

	if s.shouldCleanup(ts) {
		_ = s.cleanup(ctx, ts)
	}
	return nil
}

func (s *store) shouldCleanup(now time.Time) bool {
	if s.insertCount.Add(1)%defaultCleanupEvery == 0 {
		s.lastCleanup.Store(now.UnixNano())
		return true
	}

	nowNano := now.UnixNano()
	last := s.lastCleanup.Load()
	if nowNano-last < defaultCleanupPeriod.Nanoseconds() {
		return false
	}
	return s.lastCleanup.CompareAndSwap(last, nowNano)
}

func (s *store) cleanup(ctx context.Context, now time.Time) error {
	cutoff := now.Add(-s.retention).UnixNano()
	_, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE ts < ?`, cutoff)
	return err
}

type LogRow struct {
	ID         int64  `json:"id"`
	TS         int64  `json:"ts"` // unix nano
	Level      string `json:"level"`
	Message    string `json:"message"`
	Prefix     string `json:"prefix,omitempty"`
	Method     string `json:"method,omitempty"`
	Path       string `json:"path,omitempty"`
	Host       string `json:"host,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
	ErrorText  string `json:"error_text,omitempty"`
}

type queryParams struct {
	Levels    []string // empty = any
	Query     string   // substring on message + error_text + path + request_id
	Limit     int
	Before    int64 // id cursor: only return rows with id < Before (0 = no cursor)
	Since     int64 // unix nano lower bound (inclusive); 0 = no lower bound
	Until     int64 // unix nano upper bound (exclusive); 0 = no upper bound
	StatusMin int   // inclusive lower bound on status_code; 0 = no lower bound
	StatusMax int   // inclusive upper bound on status_code; 0 = no upper bound
}

// buildFilter renders the WHERE clause and args shared by Query and Delete.
// Returned string is empty when no filters are set; callers should NOT prefix
// with "WHERE" when empty.
func (p queryParams) buildFilter() (string, []any) {
	var (
		clauses []string
		args    []any
	)

	if len(p.Levels) > 0 {
		placeholders := make([]string, len(p.Levels))
		for i, lvl := range p.Levels {
			placeholders[i] = "?"
			args = append(args, strings.ToLower(strings.TrimSpace(lvl)))
		}
		clauses = append(clauses, "level IN ("+strings.Join(placeholders, ",")+")")
	}
	if q := strings.TrimSpace(p.Query); q != "" {
		clauses = append(clauses, `(message LIKE ? ESCAPE '\' OR error_text LIKE ? ESCAPE '\' OR path LIKE ? ESCAPE '\' OR request_id LIKE ? ESCAPE '\')`)
		like := "%" + escapeLike(q) + "%"
		args = append(args, like, like, like, like)
	}
	if p.Since > 0 {
		clauses = append(clauses, "ts >= ?")
		args = append(args, p.Since)
	}
	if p.Until > 0 {
		clauses = append(clauses, "ts < ?")
		args = append(args, p.Until)
	}
	if p.StatusMin > 0 {
		clauses = append(clauses, "status_code >= ?")
		args = append(args, p.StatusMin)
	}
	if p.StatusMax > 0 {
		clauses = append(clauses, "status_code <= ?")
		args = append(args, p.StatusMax)
	}
	if p.Before > 0 {
		clauses = append(clauses, "id < ?")
		args = append(args, p.Before)
	}

	if len(clauses) == 0 {
		return "", nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *store) Query(ctx context.Context, p queryParams) ([]LogRow, error) {
	limit := p.Limit
	if limit <= 0 {
		limit = storeDefaultQueryLim
	}
	if limit > storeMaxQueryLimit {
		limit = storeMaxQueryLimit
	}

	where, args := p.buildFilter()
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
SELECT id, ts, level, message, prefix, method, path, host, request_id, status_code, error_text
FROM logs
`+where+`
ORDER BY id DESC
LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	out := make([]LogRow, 0, limit)
	for rows.Next() {
		var r LogRow
		if err := rows.Scan(&r.ID, &r.TS, &r.Level, &r.Message, &r.Prefix, &r.Method, &r.Path, &r.Host, &r.RequestID, &r.StatusCode, &r.ErrorText); err != nil {
			return nil, fmt.Errorf("scan log row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Delete removes rows matching the same filter set as Query. Pagination
// (Limit, Before) is ignored — Delete always operates on the full matching
// set. With no filters, every row is removed. Returns the number of rows
// deleted.
func (s *store) Delete(ctx context.Context, p queryParams) (int64, error) {
	// Strip pagination — they're meaningful only for reads.
	p.Limit = 0
	p.Before = 0

	where, args := p.buildFilter()
	res, err := s.db.ExecContext(ctx, `DELETE FROM logs `+where, args...)
	if err != nil {
		return 0, fmt.Errorf("delete logs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return n, nil
}

func filterEmpty(in []string) []string {
	out := in[:0]
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

func escapeLike(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
