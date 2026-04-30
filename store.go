package logrelay

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
	storeDeleteBatchSize = 1000
)

type store struct {
	db          *sql.DB
	path        string
	retention   time.Duration
	maxBytes    int64
	insertCount atomic.Uint64
	lastCleanup atomic.Int64
}

func openStore(path string, retention time.Duration) (*store, error) {
	return openStoreWithLimit(path, retention, 0)
}

func openStoreWithLimit(path string, retention time.Duration, maxBytes int64) (*store, error) {
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
    app         TEXT    NOT NULL DEFAULT '',
    source      TEXT    NOT NULL DEFAULT '',
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
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	if err := ensureLogColumn(db, "app", "ALTER TABLE logs ADD COLUMN app TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureLogColumn(db, "source", "ALTER TABLE logs ADD COLUMN source TEXT NOT NULL DEFAULT ''"); err != nil {
		_ = db.Close()
		return nil, err
	}

	indexes := `
CREATE INDEX IF NOT EXISTS idx_logs_ts    ON logs(ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_app_ts ON logs(app, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_source_ts ON logs(source, ts DESC);
`
	if _, err := db.Exec(indexes); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init indexes: %w", err)
	}

	s := &store{db: db, path: path, retention: retention, maxBytes: maxBytes}
	now := time.Now()
	if err := s.cleanup(context.Background(), now); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cleanup old logs: %w", err)
	}
	s.lastCleanup.Store(now.UnixNano())
	return s, nil
}

func ensureLogColumn(db *sql.DB, name, ddl string) error {
	rows, err := db.Query(`PRAGMA table_info(logs)`)
	if err != nil {
		return fmt.Errorf("inspect logs schema: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			columnName string
			columnType string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultVal, &pk); err != nil {
			return fmt.Errorf("scan logs schema: %w", err)
		}
		if columnName == name {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read logs schema: %w", err)
	}
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("migrate logs schema: %w", err)
	}
	return nil
}

func (s *store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *store) Insert(ctx context.Context, ts time.Time, entry Entry, raw string) error {
	return s.InsertTagged(ctx, ts, "", "", entry, raw)
}

func (s *store) InsertTagged(ctx context.Context, ts time.Time, appName, source string, entry Entry, raw string) error {
	errorText := strings.TrimSpace(strings.Join(filterEmpty([]string{
		entry.ErrorMessage, entry.ErrMessage, entry.Err, entry.Cause,
	}), " | "))

	_, err := s.db.ExecContext(ctx, `
INSERT INTO logs (ts, app, source, level, message, prefix, method, path, host, request_id, status_code, error_text, raw)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ts.UnixNano(),
		strings.TrimSpace(appName),
		strings.TrimSpace(source),
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
		if err := s.cleanup(ctx, ts); err != nil {
			return fmt.Errorf("cleanup logs: %w", err)
		}
	} else if s.maxBytes > 0 {
		if err := s.pruneToDiskLimit(ctx); err != nil {
			return fmt.Errorf("prune logs to disk limit: %w", err)
		}
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
	if _, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE ts < ?`, cutoff); err != nil {
		return err
	}
	if s.maxBytes <= 0 {
		return nil
	}
	return s.pruneToDiskLimit(ctx)
}

func (s *store) pruneToDiskLimit(ctx context.Context) error {
	for {
		size, err := s.diskUsage()
		if err != nil {
			return err
		}
		if size <= s.maxBytes {
			return nil
		}

		deleted, err := s.deleteOldest(ctx, storeDeleteBatchSize)
		if err != nil {
			return err
		}
		if deleted == 0 {
			return nil
		}

		if err := s.compact(ctx); err != nil {
			return err
		}
	}
}

func (s *store) deleteOldest(ctx context.Context, limit int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE id IN (SELECT id FROM logs ORDER BY id ASC LIMIT ?)`, limit)
	if err != nil {
		return 0, fmt.Errorf("delete oldest logs: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("oldest rows affected: %w", err)
	}
	return deleted, nil
}

func (s *store) compact(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("checkpoint store: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return fmt.Errorf("compact store: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("checkpoint compacted store: %w", err)
	}
	return nil
}

func (s *store) diskUsage() (int64, error) {
	var total int64
	for _, path := range []string{s.path, s.path + "-wal", s.path + "-shm"} {
		info, err := os.Stat(path)
		if err == nil {
			total += info.Size()
			continue
		}
		if os.IsNotExist(err) {
			continue
		}
		return 0, fmt.Errorf("stat store file %s: %w", path, err)
	}
	return total, nil
}

type LogRow struct {
	ID         int64  `json:"id"`
	TS         int64  `json:"ts"` // unix nano
	App        string `json:"app,omitempty"`
	Source     string `json:"source,omitempty"`
	Level      string `json:"level"`
	Message    string `json:"message"`
	Prefix     string `json:"prefix,omitempty"`
	Method     string `json:"method,omitempty"`
	Path       string `json:"path,omitempty"`
	Host       string `json:"host,omitempty"`
	RequestID  string `json:"request_id,omitempty"`
	StatusCode int    `json:"status_code,omitempty"`
	ErrorText  string `json:"error_text,omitempty"`
	Raw        string `json:"raw,omitempty"`
}

type LogFilters struct {
	Apps    []string `json:"apps"`
	Sources []string `json:"sources"`
}

type queryParams struct {
	Levels    []string // empty = any
	Apps      []string // empty = any
	Sources   []string // empty = any
	Query     string   // substring on message + error_text + path + request_id + app + source
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
	if len(p.Apps) > 0 {
		placeholders := make([]string, len(p.Apps))
		for i, app := range p.Apps {
			placeholders[i] = "?"
			args = append(args, strings.TrimSpace(app))
		}
		clauses = append(clauses, "app IN ("+strings.Join(placeholders, ",")+")")
	}
	if len(p.Sources) > 0 {
		placeholders := make([]string, len(p.Sources))
		for i, source := range p.Sources {
			placeholders[i] = "?"
			args = append(args, strings.TrimSpace(source))
		}
		clauses = append(clauses, "source IN ("+strings.Join(placeholders, ",")+")")
	}
	if q := strings.TrimSpace(p.Query); q != "" {
		clauses = append(clauses, `(message LIKE ? ESCAPE '\' OR error_text LIKE ? ESCAPE '\' OR path LIKE ? ESCAPE '\' OR request_id LIKE ? ESCAPE '\' OR app LIKE ? ESCAPE '\' OR source LIKE ? ESCAPE '\')`)
		like := "%" + escapeLike(q) + "%"
		args = append(args, like, like, like, like, like, like)
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
SELECT id, ts, app, source, level, message, prefix, method, path, host, request_id, status_code, error_text, raw
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
		if err := rows.Scan(&r.ID, &r.TS, &r.App, &r.Source, &r.Level, &r.Message, &r.Prefix, &r.Method, &r.Path, &r.Host, &r.RequestID, &r.StatusCode, &r.ErrorText, &r.Raw); err != nil {
			return nil, fmt.Errorf("scan log row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *store) Filters(ctx context.Context) (LogFilters, error) {
	apps, err := s.distinctText(ctx, "app")
	if err != nil {
		return LogFilters{}, err
	}
	sources, err := s.distinctText(ctx, "source")
	if err != nil {
		return LogFilters{}, err
	}
	return LogFilters{Apps: apps, Sources: sources}, nil
}

func (s *store) distinctText(ctx context.Context, column string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT `+column+` FROM logs WHERE `+column+` <> '' ORDER BY `+column)
	if err != nil {
		return nil, fmt.Errorf("query distinct %s: %w", column, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan distinct %s: %w", column, err)
		}
		out = append(out, value)
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
