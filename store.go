package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteTimestampLayout = "2006-01-02T15:04:05.000000000Z07:00"

// The writer runs in WAL mode so the readers never block it. busy_timeout makes
// a database locked by another process wait instead of failing at once with
// SQLITE_BUSY (the driver default is 0). synchronous(NORMAL) is safe in WAL:
// a power loss may drop the last commits but never corrupts the database.
// query_only keeps the read pool from ever writing.
const (
	// _txlock=immediate takes the write lock at BEGIN; journal_size_limit
	// truncates the WAL back after a checkpoint (one cleanup batch can grow it).
	sqliteWriterParams = "?_pragma=busy_timeout(3000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)" +
		"&_pragma=journal_size_limit(8388608)&_txlock=immediate"
	sqliteReaderParams = "?_pragma=busy_timeout(3000)&_pragma=query_only(1)"
	readPoolSize       = 4
	// cleanupBatchSize bounds how long one retention batch holds the writer.
	cleanupBatchSize = 10000
)

// recordColumns is the column list scanned by scanRequestDetail, in order.
const recordColumns = `id, timestamp, api_key, provider, model, alias, source, auth_id, auth_index, auth_type, executor_type,
       reasoning_effort, service_tier, latency_ms, ttft_ms,
       input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens, total_tokens,
       failed, failure_status_code, failure_body, cost_usd`

// errInvalidCursor reports a records cursor that was not produced by this plugin.
var errInvalidCursor = errors.New("invalid cursor")

// SQLiteStore persists usage records in a pure-Go SQLite database.
type SQLiteStore struct {
	// db is the single writer connection; rdb is the read-only pool.
	db  *sql.DB
	rdb *sql.DB

	// prices caches model_prices so cost math never touches the single connection.
	pricesMu sync.RWMutex
	prices   map[string]ModelPrice
	// replaceMu orders whole-table price saves with their cache updates.
	replaceMu sync.Mutex
}

// NewSQLiteStore opens (creating if needed) the usage database at path.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("usage sqlite path is empty")
	}
	if err := prepareSQLitePath(path); err != nil {
		return nil, err
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite", path+sqliteWriterParams)
	if err != nil {
		return nil, fmt.Errorf("usage sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &SQLiteStore{db: db}
	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
		_ = db.Close()
		return nil, fmt.Errorf("usage sqlite journal mode is %q, want wal: %v", mode, err)
	}
	if err := store.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Prices only affect cost display; never let them stop usage recording.
	if err := store.loadPrices(ctx); err != nil {
		store.setPrices(map[string]ModelPrice{})
	}
	// Opened after the writer has switched the file to WAL.
	rdb, err := sql.Open("sqlite", path+sqliteReaderParams)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("usage sqlite open readers: %w", err)
	}
	rdb.SetMaxOpenConns(readPoolSize)
	rdb.SetMaxIdleConns(readPoolSize)
	store.rdb = rdb
	return store, nil
}

func prepareSQLitePath(path string) error {
	dir := filepath.Clean(filepath.Dir(path))
	if dir != "." && filepath.Dir(dir) != dir {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("usage sqlite mkdir: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("usage sqlite create: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("usage sqlite close created file: %w", err)
	}
	return nil
}

// initSchema creates the table and indexes. Column names are aligned with
// upstream usage.Record / usage.Detail; there is no legacy migration path.
func (s *SQLiteStore) initSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS usage_records (
	id TEXT PRIMARY KEY,
	timestamp TEXT NOT NULL,
	api_key TEXT NOT NULL DEFAULT '',
	provider TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	alias TEXT NOT NULL DEFAULT '',
	source TEXT NOT NULL DEFAULT '',
	auth_id TEXT NOT NULL DEFAULT '',
	auth_index TEXT NOT NULL DEFAULT '',
	auth_type TEXT NOT NULL DEFAULT '',
	executor_type TEXT NOT NULL DEFAULT '',
	reasoning_effort TEXT NOT NULL DEFAULT '',
	service_tier TEXT NOT NULL DEFAULT '',
	latency_ms INTEGER NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
	ttft_ms INTEGER NOT NULL DEFAULT 0 CHECK (ttft_ms >= 0),
	input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
	output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
	reasoning_tokens INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_tokens >= 0),
	cached_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cached_tokens >= 0),
	cache_read_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_read_tokens >= 0),
	cache_creation_tokens INTEGER NOT NULL DEFAULT 0 CHECK (cache_creation_tokens >= 0),
	total_tokens INTEGER NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
	failed INTEGER NOT NULL DEFAULT 0 CHECK (failed IN (0, 1)),
	failure_status_code INTEGER NOT NULL DEFAULT 0 CHECK (failure_status_code >= 0),
	failure_body TEXT NOT NULL DEFAULT '',
	cost_usd REAL
)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_records_timestamp ON usage_records(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_records_api_model ON usage_records(api_key, provider, model)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("usage sqlite init schema: %w", err)
		}
	}
	if err := s.migrateSchema(ctx); err != nil {
		return err
	}
	// Backs per-account paging. Created after migrateSchema because older dbs may
	// lack auth_index. Index only, no new columns, so v0.1.0 can still open the db.
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_usage_records_auth_ts ON usage_records(auth_index, timestamp, id)`); err != nil {
		return fmt.Errorf("usage sqlite init schema: %w", err)
	}
	if err := s.initPriceSchema(ctx); err != nil {
		return err
	}
	if err := s.initHealthSchema(ctx); err != nil {
		return err
	}
	// Last: the rollup triggers read columns migrateSchema may have just added.
	return s.initRollup(ctx)
}

// migrateSchema 让老版本 db 的字段自愈到最新：用 table_info 读出现有列，
// 对最新 schema 中缺失的列逐个 ALTER TABLE ADD COLUMN 补齐（SQLite 不能重复
// ADD，同名列跳过）。id/timestamp 是建表原始列（任何版本都有）且不可 ADD，
// 不在补齐范围。ADD COLUMN 只带 NOT NULL DEFAULT、不带 CHECK（SQLite 对
// ADD COLUMN 的约束限制），数值非负由写入侧 nonNegative 保证。
func (s *SQLiteStore) migrateSchema(ctx context.Context) error {
	existing, err := s.existingColumns(ctx)
	if err != nil {
		return err
	}
	additions := []struct {
		name string
		ddl  string
	}{
		{"api_key", `ALTER TABLE usage_records ADD COLUMN api_key TEXT NOT NULL DEFAULT ''`},
		{"provider", `ALTER TABLE usage_records ADD COLUMN provider TEXT NOT NULL DEFAULT ''`},
		{"model", `ALTER TABLE usage_records ADD COLUMN model TEXT NOT NULL DEFAULT ''`},
		{"alias", `ALTER TABLE usage_records ADD COLUMN alias TEXT NOT NULL DEFAULT ''`},
		{"source", `ALTER TABLE usage_records ADD COLUMN source TEXT NOT NULL DEFAULT ''`},
		{"auth_id", `ALTER TABLE usage_records ADD COLUMN auth_id TEXT NOT NULL DEFAULT ''`},
		{"auth_index", `ALTER TABLE usage_records ADD COLUMN auth_index TEXT NOT NULL DEFAULT ''`},
		{"auth_type", `ALTER TABLE usage_records ADD COLUMN auth_type TEXT NOT NULL DEFAULT ''`},
		{"executor_type", `ALTER TABLE usage_records ADD COLUMN executor_type TEXT NOT NULL DEFAULT ''`},
		{"reasoning_effort", `ALTER TABLE usage_records ADD COLUMN reasoning_effort TEXT NOT NULL DEFAULT ''`},
		{"service_tier", `ALTER TABLE usage_records ADD COLUMN service_tier TEXT NOT NULL DEFAULT ''`},
		{"latency_ms", `ALTER TABLE usage_records ADD COLUMN latency_ms INTEGER NOT NULL DEFAULT 0`},
		{"ttft_ms", `ALTER TABLE usage_records ADD COLUMN ttft_ms INTEGER NOT NULL DEFAULT 0`},
		{"input_tokens", `ALTER TABLE usage_records ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0`},
		{"output_tokens", `ALTER TABLE usage_records ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0`},
		{"reasoning_tokens", `ALTER TABLE usage_records ADD COLUMN reasoning_tokens INTEGER NOT NULL DEFAULT 0`},
		{"cached_tokens", `ALTER TABLE usage_records ADD COLUMN cached_tokens INTEGER NOT NULL DEFAULT 0`},
		{"cache_read_tokens", `ALTER TABLE usage_records ADD COLUMN cache_read_tokens INTEGER NOT NULL DEFAULT 0`},
		{"cache_creation_tokens", `ALTER TABLE usage_records ADD COLUMN cache_creation_tokens INTEGER NOT NULL DEFAULT 0`},
		{"total_tokens", `ALTER TABLE usage_records ADD COLUMN total_tokens INTEGER NOT NULL DEFAULT 0`},
		{"failed", `ALTER TABLE usage_records ADD COLUMN failed INTEGER NOT NULL DEFAULT 0`},
		{"failure_status_code", `ALTER TABLE usage_records ADD COLUMN failure_status_code INTEGER NOT NULL DEFAULT 0`},
		{"failure_body", `ALTER TABLE usage_records ADD COLUMN failure_body TEXT NOT NULL DEFAULT ''`},
		// The cost of a record at the price in effect when it was written; NULL when its model
		// had no price then. Nullable on purpose: NULL (unpriced) and 0 (free) differ.
		{"cost_usd", `ALTER TABLE usage_records ADD COLUMN cost_usd REAL`},
	}
	for _, addition := range additions {
		if _, ok := existing[addition.name]; ok {
			continue
		}
		if _, err := s.db.ExecContext(ctx, addition.ddl); err != nil {
			return fmt.Errorf("usage sqlite migrate add %s: %w", addition.name, err)
		}
	}
	return nil
}

// existingColumns 返回 usage_records 当前已有的列名集合。
func (s *SQLiteStore) existingColumns(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(usage_records)")
	if err != nil {
		return nil, fmt.Errorf("usage sqlite table_info: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns := make(map[string]struct{})
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("usage sqlite table_info scan: %w", err)
		}
		columns[name] = struct{}{}
	}
	return columns, rows.Err()
}

// Insert stores one usage record.
func (s *SQLiteStore) Insert(ctx context.Context, record Record) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage sqlite store is nil")
	}
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("usage record id is empty")
	}

	tokens := nonNegativeTokenStats(record.Tokens)
	tokens.TotalTokens = normalizeTotalTokens(tokens)

	// The cost is fixed now, at the price in effect for this request: a later price change
	// only prices the requests after it. NULL when the model has no price yet.
	model := normalizeModel(record.Model)
	var cost any
	if price, ok := s.priceFor(model); ok {
		cost = costUSD(price, pureInputTokens(record.Provider, tokens), tokens.CachedTokens, tokens.CacheCreationTokens, tokens.OutputTokens)
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO usage_records (
	id, timestamp, api_key, provider, model, alias, source, auth_id, auth_index, auth_type, executor_type,
	reasoning_effort, service_tier, latency_ms, ttft_ms,
	input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens, total_tokens,
	failed, failure_status_code, failure_body, cost_usd
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		strings.TrimSpace(record.ID),
		formatRecordTimestamp(record.Timestamp),
		strings.TrimSpace(record.APIKey),
		strings.TrimSpace(record.Provider),
		model,
		strings.TrimSpace(record.Alias),
		strings.TrimSpace(record.Source),
		strings.TrimSpace(record.AuthID),
		strings.TrimSpace(record.AuthIndex),
		strings.TrimSpace(record.AuthType),
		strings.TrimSpace(record.ExecutorType),
		strings.TrimSpace(record.ReasoningEffort),
		strings.TrimSpace(record.ServiceTier),
		nonNegative(record.LatencyMs),
		nonNegative(record.TTFTMs),
		tokens.InputTokens,
		tokens.OutputTokens,
		tokens.ReasoningTokens,
		tokens.CachedTokens,
		tokens.CacheReadTokens,
		tokens.CacheCreationTokens,
		tokens.TotalTokens,
		boolToInt(record.Failed),
		nonNegativeInt(record.FailureStatusCode),
		strings.TrimSpace(record.FailureBody),
		cost,
	)
	if err != nil {
		return fmt.Errorf("usage sqlite insert: %w", err)
	}
	return nil
}

// Query returns usage grouped by api_key (or provider) then model.
func (s *SQLiteStore) Query(ctx context.Context, rng QueryRange) (APIUsage, error) {
	if s == nil || s.rdb == nil {
		return APIUsage{}, nil
	}
	where, args := rangeWhere(rng)
	query := "SELECT " + recordColumns + " FROM usage_records"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY timestamp ASC, id ASC"

	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("usage sqlite query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := APIUsage{}
	for rows.Next() {
		detail, apiKey, _, _, err := scanRequestDetail(rows)
		if err != nil {
			return nil, err
		}
		key := groupingKey(apiKey, detail.Provider)
		modelKey := normalizeModel(detail.Model)
		if result[key] == nil {
			result[key] = map[string][]RequestDetail{}
		}
		result[key][modelKey] = append(result[key][modelKey], detail)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("usage sqlite rows: %w", err)
	}
	return result, nil
}

// AccountSummary aggregates usage per credential (auth_index + auth_id) within
// rng, ordered by total tokens descending. Whole days and hours come from the
// rollups and only the partial hours at the ends from the details (see
// planSummaryPieces), all read in one snapshot. Groups stay per model so each
// model's price applies to its own tokens; they are merged per account here.
func (s *SQLiteStore) AccountSummary(ctx context.Context, rng QueryRange) (AccountsResponse, error) {
	result := AccountsResponse{Accounts: []AccountUsage{}}
	if s == nil || s.rdb == nil {
		return result, nil
	}
	// One read transaction: every piece sees the same WAL snapshot.
	tx, err := s.rdb.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("usage sqlite account summary begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	since, err := detailsSince(ctx, tx)
	if err != nil {
		return result, err
	}
	result.DetailsSince = since
	// A zero time is an open bound, as in rangeWhere (/usage, /records).
	start, end := rng.Start, rng.End
	if start != nil && start.IsZero() {
		start = nil
	}
	if end != nil && end.IsZero() {
		end = nil
	}
	groups := make(map[summaryGroupKey]*summaryGroup)
	for _, piece := range planSummaryPieces(start, end, since) {
		if err := scanSummaryPiece(ctx, tx, piece, groups); err != nil {
			return result, err
		}
	}
	// Same order the per-model SQL GROUP BY produced, so float cost sums match.
	keys := make([]summaryGroupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.authIndex != b.authIndex {
			return a.authIndex < b.authIndex
		}
		if a.authID != b.authID {
			return a.authID < b.authID
		}
		return a.model < b.model
	})

	type accountKey struct{ authIndex, authID string }
	accounts := make(map[accountKey]*AccountUsage)
	for _, gk := range keys {
		g := groups[gk]
		model := g.model
		group := AccountUsage{
			AuthIndex: g.authIndex, AuthID: g.authID, Source: g.source, Provider: g.provider, AuthType: g.authType,
			Requests: g.requests, Failed: g.failed, Tokens: g.tokens,
		}
		if group.FirstRequestAt, err = parseStoredTimestamp(g.first); err != nil {
			return result, err
		}
		if group.LastRequestAt, err = parseStoredTimestamp(g.last); err != nil {
			return result, err
		}
		// Taken before merging: the first group becomes the account itself.
		modelUsage := ModelUsage{
			Model: model, Requests: group.Requests, Success: group.Requests - group.Failed,
			Failed: group.Failed, Tokens: group.Tokens,
		}
		key := accountKey{group.AuthIndex, group.AuthID}
		account, ok := accounts[key]
		if !ok {
			group.UnpricedModels = []string{}
			group.Models = []ModelUsage{}
			account = &group
			accounts[key] = account
		} else {
			mergeAccountGroup(account, group)
		}
		// Costs were fixed when each request was written, at the price in effect then.
		// A model counts as unpriced while any of its requests was written without a price.
		if g.requests > g.unpriced {
			account.CostUSD += g.cost
			rounded := roundUSD(g.cost)
			modelUsage.CostUSD = &rounded
		}
		if g.unpriced > 0 {
			account.UnpricedModels = append(account.UnpricedModels, model)
		}
		account.Models = append(account.Models, modelUsage)
	}

	for _, account := range accounts {
		account.Success = account.Requests - account.Failed
		account.CostUSD = roundUSD(account.CostUSD)
		sort.Strings(account.UnpricedModels)
		sort.Slice(account.Models, func(i, j int) bool {
			a, b := account.Models[i], account.Models[j]
			if a.Tokens.TotalTokens != b.Tokens.TotalTokens {
				return a.Tokens.TotalTokens > b.Tokens.TotalTokens
			}
			return a.Model < b.Model
		})
		result.Accounts = append(result.Accounts, *account)
	}
	sort.Slice(result.Accounts, func(i, j int) bool {
		a, b := result.Accounts[i], result.Accounts[j]
		if a.Tokens.TotalTokens != b.Tokens.TotalTokens {
			return a.Tokens.TotalTokens > b.Tokens.TotalTokens
		}
		if a.AuthID != b.AuthID {
			return a.AuthID < b.AuthID
		}
		return a.AuthIndex < b.AuthIndex
	})
	for _, account := range result.Accounts {
		addTotals(&result.Totals, account)
	}
	result.Totals.CostUSD = roundUSD(result.Totals.CostUSD)
	result.Totals.Accounts = len(result.Accounts)
	return result, nil
}

// mergeAccountGroup folds one per-model group into its account, keeping the
// same MAX/MIN semantics the per-account SQL used before.
func mergeAccountGroup(account *AccountUsage, group AccountUsage) {
	account.Source = max(account.Source, group.Source)
	account.Provider = max(account.Provider, group.Provider)
	account.AuthType = max(account.AuthType, group.AuthType)
	account.Requests += group.Requests
	account.Failed += group.Failed
	addTokens(&account.Tokens, group.Tokens)
	if group.FirstRequestAt.Before(account.FirstRequestAt) {
		account.FirstRequestAt = group.FirstRequestAt
	}
	if group.LastRequestAt.After(account.LastRequestAt) {
		account.LastRequestAt = group.LastRequestAt
	}
}

// ListRecords returns one page of records newest first, optionally filtered by
// credential, model and result. The cursor is a (timestamp, id) keyset: fine
// for browsing, but a long request is written after rows that started later,
// so it is not a safe watermark for incremental sync. Numbered paging (q.Page)
// uses OFFSET and also counts the matching records.
func (s *SQLiteStore) ListRecords(ctx context.Context, q RecordsQuery) (RecordsPage, error) {
	page := RecordsPage{Records: []RecordDetail{}}
	if q.Page > 0 {
		var zero int64
		page.Total = &zero
	}
	if s == nil || s.rdb == nil {
		return page, nil
	}
	if q.Limit <= 0 {
		return page, fmt.Errorf("usage sqlite list records: limit must be positive")
	}
	// The count and the page come from one snapshot, so they always agree.
	tx, err := s.rdb.BeginTx(ctx, nil)
	if err != nil {
		return page, fmt.Errorf("usage sqlite list records begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	where, args := rangeWhere(q.Range)
	if q.AuthIndex != "" {
		where = append(where, "auth_index = ?")
		args = append(args, q.AuthIndex)
	}
	if q.AuthID != "" {
		where = append(where, "auth_id = ?")
		args = append(args, q.AuthID)
	}
	if q.Model != "" {
		where = append(where, "model = ?")
		args = append(args, q.Model)
	}
	if q.Failed != nil {
		where = append(where, "failed = ?")
		args = append(args, boolToInt(*q.Failed))
	}
	offset := 0
	if q.Page > 0 {
		countQuery := "SELECT COUNT(*) FROM usage_records"
		if len(where) > 0 {
			countQuery += " WHERE " + strings.Join(where, " AND ")
		}
		if err := tx.QueryRowContext(ctx, countQuery, args...).Scan(page.Total); err != nil {
			return page, fmt.Errorf("usage sqlite count records: %w", err)
		}
		offset = (q.Page - 1) * q.Limit
		// Past the end: skip the page query, which would walk every match.
		if int64(offset) >= *page.Total {
			return page, nil
		}
	}
	if q.Cursor != "" {
		cursorTimestamp, cursorID, err := decodeRecordsCursor(q.Cursor)
		if err != nil {
			return page, err
		}
		// Row-value form keeps the index range scan; the equivalent OR form makes
		// SQLite collect and sort every older row. Both columns are NOT NULL.
		where = append(where, "(timestamp, id) < (?, ?)")
		args = append(args, cursorTimestamp, cursorID)
	}
	query := "SELECT " + recordColumns + " FROM usage_records"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to learn whether another page exists.
	query += " ORDER BY timestamp DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, q.Limit+1, offset)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return page, fmt.Errorf("usage sqlite list records: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var lastTimestampText string
	for rows.Next() {
		detail, _, timestampText, cost, err := scanRequestDetail(rows)
		if err != nil {
			return page, err
		}
		if len(page.Records) == q.Limit {
			next := encodeRecordsCursor(lastTimestampText, page.Records[len(page.Records)-1].ID)
			page.NextCursor = &next
			break
		}
		page.Records = append(page.Records, RecordDetail{RequestDetail: detail, CostUSD: cost})
		lastTimestampText = timestampText
	}
	if err := rows.Err(); err != nil {
		return page, fmt.Errorf("usage sqlite list records rows: %w", err)
	}
	return page, nil
}

// rangeWhere turns a QueryRange into SQL conditions on the timestamp column.
func rangeWhere(rng QueryRange) ([]string, []any) {
	where := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if rng.Start != nil && !rng.Start.IsZero() {
		where = append(where, "timestamp >= ?")
		args = append(args, formatTimestamp(*rng.Start))
	}
	if rng.End != nil && !rng.End.IsZero() {
		where = append(where, "timestamp < ?")
		args = append(args, formatTimestamp(*rng.End))
	}
	return where, args
}

// scanRequestDetail scans one row selected with recordColumns. It also returns
// the api_key (not part of RequestDetail), the raw stored timestamp text and the
// cost fixed when the record was written (nil when its model had no price then).
func scanRequestDetail(rows *sql.Rows) (RequestDetail, string, string, *float64, error) {
	var timestampText string
	var apiKey string
	var failedInt int
	var storedCost sql.NullFloat64
	detail := RequestDetail{}
	if err := rows.Scan(
		&detail.ID,
		&timestampText,
		&apiKey,
		&detail.Provider,
		&detail.Model,
		&detail.Alias,
		&detail.Source,
		&detail.AuthID,
		&detail.AuthIndex,
		&detail.AuthType,
		&detail.ExecutorType,
		&detail.ReasoningEffort,
		&detail.ServiceTier,
		&detail.LatencyMs,
		&detail.TTFTMs,
		&detail.Tokens.InputTokens,
		&detail.Tokens.OutputTokens,
		&detail.Tokens.ReasoningTokens,
		&detail.Tokens.CachedTokens,
		&detail.Tokens.CacheReadTokens,
		&detail.Tokens.CacheCreationTokens,
		&detail.Tokens.TotalTokens,
		&failedInt,
		&detail.FailureStatusCode,
		&detail.FailureBody,
		&storedCost,
	); err != nil {
		return detail, "", "", nil, fmt.Errorf("usage sqlite scan: %w", err)
	}
	parsed, err := parseStoredTimestamp(timestampText)
	if err != nil {
		return detail, "", "", nil, err
	}
	detail.Timestamp = parsed
	detail.LatencyMs = nonNegative(detail.LatencyMs)
	detail.TTFTMs = nonNegative(detail.TTFTMs)
	detail.Failed = failedInt != 0
	var cost *float64
	if storedCost.Valid {
		rounded := roundUSD(storedCost.Float64)
		cost = &rounded
	}
	return detail, apiKey, timestampText, cost, nil
}

func parseStoredTimestamp(text string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("usage sqlite parse timestamp: %w", err)
	}
	return parsed.UTC(), nil
}

func addTotals(totals *UsageTotals, account AccountUsage) {
	totals.Requests += account.Requests
	totals.Success += account.Success
	totals.Failed += account.Failed
	addTokens(&totals.Tokens, account.Tokens)
	totals.CostUSD += account.CostUSD
}

func addTokens(dst *TokenStats, src TokenStats) {
	dst.InputTokens += src.InputTokens
	dst.OutputTokens += src.OutputTokens
	dst.ReasoningTokens += src.ReasoningTokens
	dst.CachedTokens += src.CachedTokens
	dst.CacheReadTokens += src.CacheReadTokens
	dst.CacheCreationTokens += src.CacheCreationTokens
	dst.TotalTokens += src.TotalTokens
}

// encodeRecordsCursor packs the last row's stored timestamp text and id.
func encodeRecordsCursor(timestampText, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(timestampText + "|" + id))
}

func decodeRecordsCursor(cursor string) (string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", errInvalidCursor
	}
	timestampText, id, ok := strings.Cut(string(raw), "|")
	if !ok || id == "" {
		return "", "", errInvalidCursor
	}
	// Reject anything that is not in the exact stored form (e.g. a +08:00 offset),
	// since cursors are compared to stored timestamps as text.
	parsed, err := time.Parse(sqliteTimestampLayout, timestampText)
	if err != nil || formatTimestamp(parsed) != timestampText {
		return "", "", errInvalidCursor
	}
	return timestampText, id, nil
}

// Delete removes records by id and reports which ids were absent.
func (s *SQLiteStore) Delete(ctx context.Context, ids []string) (DeleteResult, error) {
	result := DeleteResult{Missing: []string{}}
	if s == nil || s.db == nil {
		result.Missing = append(result.Missing, ids...)
		return result, nil
	}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		res, err := s.db.ExecContext(ctx, "DELETE FROM usage_records WHERE id = ?", id)
		if err != nil {
			return result, fmt.Errorf("usage sqlite delete %s: %w", id, err)
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return result, fmt.Errorf("usage sqlite rows affected: %w", err)
		}
		if rows == 0 {
			result.Missing = append(result.Missing, id)
			continue
		}
		result.Deleted += rows
	}
	return result, nil
}

// DeleteBefore removes records older than cutoff for retention and returns the
// deleted count. The rollups keep them: each batch turns the delete trigger off
// inside its own transaction, so the switch never outlives it. Batches release
// the writer in between, letting queued inserts through.
func (s *SQLiteStore) DeleteBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	var total int64
	for {
		deleted, err := s.deleteBatchBefore(ctx, formatTimestamp(cutoff))
		total += deleted
		if err != nil || deleted < cleanupBatchSize {
			return total, err
		}
	}
}

func (s *SQLiteStore) deleteBatchBefore(ctx context.Context, cutoff string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("usage sqlite delete before begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// Without the control row the trigger would read 0 and retention would
	// erase rollup history, so refuse instead.
	flag, err := tx.ExecContext(ctx, `UPDATE usage_rollup_control SET keep_on_delete = 1 WHERE id = 1`)
	if err != nil {
		return 0, fmt.Errorf("usage sqlite delete before keep rollups: %w", err)
	}
	if n, err := flag.RowsAffected(); err != nil || n != 1 {
		return 0, fmt.Errorf("usage sqlite delete before: rollup control row missing (%d rows, %v)", n, err)
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM usage_records WHERE rowid IN (SELECT rowid FROM usage_records WHERE timestamp < ? ORDER BY timestamp LIMIT ?)`,
		cutoff, cleanupBatchSize)
	if err != nil {
		return 0, fmt.Errorf("usage sqlite delete before: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("usage sqlite rows affected: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE usage_rollup_control SET keep_on_delete = 0 WHERE id = 1`); err != nil {
		return 0, fmt.Errorf("usage sqlite delete before restore: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("usage sqlite delete before commit: %w", err)
	}
	return deleted, nil
}

// Close closes the readers, then the writer, each only after its in-flight
// work has finished, so this instance holds no file descriptor on usage.db
// when Close returns (two SQLite copies in one process must never overlap).
// The writer closes last and checkpoints the WAL back into usage.db.
func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	var errRead error
	if s.rdb != nil {
		errRead = closeWhenIdle(s.rdb, readPoolSize)
	}
	return errors.Join(errRead, closeWhenIdle(s.db, 1))
}

// closeWhenIdle closes db after its running work. db.Close closes only idle
// connections: a running statement, tx or open rows would keep using usage.db
// after it returns. Taking every connection first waits for that work. conns
// must be the pool's MaxOpenConns: below it, Conn opens a fresh connection
// instead of waiting for a busy one (so idle readers may be opened here only
// to be closed; that is the price of the guarantee).
func closeWhenIdle(db *sql.DB, conns int) error {
	ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
	defer cancel()
	held := make([]*sql.Conn, 0, conns)
	for range conns {
		conn, err := db.Conn(ctx)
		if err != nil {
			break
		}
		held = append(held, conn)
	}
	errClose := db.Close()
	for _, conn := range held {
		// The db is closed, so releasing the conn closes the driver connection.
		_ = conn.Close()
	}
	return errClose
}

func formatTimestamp(timestamp time.Time) string {
	return timestamp.UTC().Format(sqliteTimestampLayout)
}

func formatRecordTimestamp(timestamp time.Time) string {
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	return formatTimestamp(timestamp)
}

func groupingKey(apiKey, provider string) string {
	if trimmed := strings.TrimSpace(apiKey); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(provider); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

func normalizeModel(model string) string {
	if trimmed := strings.TrimSpace(model); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

func normalizeTotalTokens(tokens TokenStats) int64 {
	if tokens.TotalTokens != 0 {
		return tokens.TotalTokens
	}
	total := tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens
	if total != 0 {
		return total
	}
	return tokens.InputTokens + tokens.OutputTokens + tokens.ReasoningTokens + tokens.CachedTokens
}

func nonNegativeTokenStats(tokens TokenStats) TokenStats {
	tokens.InputTokens = nonNegative(tokens.InputTokens)
	tokens.OutputTokens = nonNegative(tokens.OutputTokens)
	tokens.ReasoningTokens = nonNegative(tokens.ReasoningTokens)
	tokens.CachedTokens = nonNegative(tokens.CachedTokens)
	tokens.CacheReadTokens = nonNegative(tokens.CacheReadTokens)
	tokens.CacheCreationTokens = nonNegative(tokens.CacheCreationTokens)
	tokens.TotalTokens = nonNegative(tokens.TotalTokens)
	return tokens
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func nonNegativeInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
