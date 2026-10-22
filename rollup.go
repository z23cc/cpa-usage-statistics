package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// rollupSchemaVersion is stored in PRAGMA user_version once the hourly and
// daily rollups, their triggers and the backfill are in place.
const rollupSchemaVersion = 1

const (
	hourBucketLayout = "2006-01-02T15"
	dayBucketLayout  = "2006-01-02"
)

// rollupTable is one rollup level. Its bucket is a prefix of the stored UTC
// timestamp text, so buckets sort like time.
type rollupTable struct {
	name      string
	bucketLen int
}

var (
	hourlyRollup  = rollupTable{name: "usage_hourly", bucketLen: len(hourBucketLayout)}
	dailyRollup   = rollupTable{name: "usage_daily", bucketLen: len(dayBucketLayout)}
	rollupTables  = []rollupTable{hourlyRollup, dailyRollup}
	rawSummaryKey = "usage_records"
	// cleanedHourKey marks a piece holding the records of one hour whose details
	// retention already deleted: that hour's rollup minus its kept details.
	cleanedHourKey = "cleaned_hour"
)

// rollupSumColumns are the usage_records columns summed into the rollups.
var rollupSumColumns = []string{
	"failed", "input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens",
	"cache_read_tokens", "cache_creation_tokens", "total_tokens",
}

// rollupKeepOnDelete reads the retention switch. COALESCE keeps manual deletes
// counted even if the control row were ever missing.
const rollupKeepOnDelete = `COALESCE((SELECT keep_on_delete FROM usage_rollup_control WHERE id = 1), 0)`

// initRollup creates the rollups, their triggers and backfills them from the
// existing records in one transaction, so no record is missed or counted
// twice. usage_records rows are only ever inserted or deleted (never updated),
// so insert and delete triggers keep the rollups exact. The triggers live in
// the database: an older plugin version writing after a rollback still keeps
// the rollups current.
func (s *SQLiteStore) initRollup(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("usage sqlite rollup version: %w", err)
	}
	if version >= rollupSchemaVersion {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("usage sqlite rollup begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		rollupTableDDL(hourlyRollup),
		rollupTableDDL(dailyRollup),
		`CREATE TABLE IF NOT EXISTS usage_rollup_control (id INTEGER PRIMARY KEY CHECK (id = 1), keep_on_delete INTEGER NOT NULL DEFAULT 0)`,
		`INSERT OR IGNORE INTO usage_rollup_control (id, keep_on_delete) VALUES (1, 0)`,
		`DROP TRIGGER IF EXISTS usage_records_rollup_ai`,
		`DROP TRIGGER IF EXISTS usage_records_rollup_ad`,
		rollupInsertTrigger(),
		rollupDeleteTrigger(),
		`DELETE FROM usage_hourly`,
		`DELETE FROM usage_daily`,
		rollupBackfillSQL(hourlyRollup),
		rollupBackfillSQL(dailyRollup),
		fmt.Sprintf(`PRAGMA user_version = %d`, rollupSchemaVersion),
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("usage sqlite rollup init: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("usage sqlite rollup commit: %w", err)
	}
	return nil
}

func rollupTableDDL(t rollupTable) string {
	columns := []string{
		"bucket TEXT NOT NULL", "auth_index TEXT NOT NULL", "auth_id TEXT NOT NULL", "model TEXT NOT NULL",
		"source TEXT NOT NULL DEFAULT ''", "provider TEXT NOT NULL DEFAULT ''", "auth_type TEXT NOT NULL DEFAULT ''",
		"requests INTEGER NOT NULL DEFAULT 0",
	}
	for _, column := range rollupSumColumns {
		columns = append(columns, column+" INTEGER NOT NULL DEFAULT 0")
	}
	columns = append(columns,
		"pure_input_tokens INTEGER NOT NULL DEFAULT 0",
		"first_ts TEXT NOT NULL", "last_ts TEXT NOT NULL",
		"PRIMARY KEY (bucket, auth_index, auth_id, model)")
	return "CREATE TABLE IF NOT EXISTS " + t.name + " (\n\t" + strings.Join(columns, ",\n\t") + "\n) WITHOUT ROWID"
}

func rollupInsertTrigger() string {
	return "CREATE TRIGGER usage_records_rollup_ai AFTER INSERT ON usage_records BEGIN\n" +
		rollupUpsertSQL(hourlyRollup) + ";\n" + rollupUpsertSQL(dailyRollup) + ";\nEND"
}

func rollupDeleteTrigger() string {
	return "CREATE TRIGGER usage_records_rollup_ad AFTER DELETE ON usage_records WHEN " + rollupKeepOnDelete + " = 0 BEGIN\n" +
		rollupDecrementSQL(hourlyRollup) + ";\n" + rollupDecrementSQL(dailyRollup) + ";\nEND"
}

// rollupUpsertSQL adds NEW to its bucket. first_ts/last_ts and the MAX'ed
// strings follow the same semantics as the per-account summary.
func rollupUpsertSQL(t rollupTable) string {
	columns := []string{"bucket", "auth_index", "auth_id", "model", "source", "provider", "auth_type", "requests"}
	values := []string{fmt.Sprintf("substr(NEW.timestamp, 1, %d)", t.bucketLen),
		"NEW.auth_index", "NEW.auth_id", "NEW.model", "NEW.source", "NEW.provider", "NEW.auth_type", "1"}
	updates := []string{"requests = requests + 1",
		"source = max(source, excluded.source)", "provider = max(provider, excluded.provider)",
		"auth_type = max(auth_type, excluded.auth_type)"}
	for _, column := range rollupSumColumns {
		columns = append(columns, column)
		values = append(values, "NEW."+column)
		updates = append(updates, column+" = "+column+" + excluded."+column)
	}
	columns = append(columns, "pure_input_tokens", "first_ts", "last_ts")
	values = append(values, pureInputExpr("NEW."), "NEW.timestamp", "NEW.timestamp")
	updates = append(updates, "pure_input_tokens = pure_input_tokens + excluded.pure_input_tokens",
		"first_ts = min(first_ts, excluded.first_ts)", "last_ts = max(last_ts, excluded.last_ts)")
	return "INSERT INTO " + t.name + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ")\n" +
		"ON CONFLICT (bucket, auth_index, auth_id, model) DO UPDATE SET " + strings.Join(updates, ", ")
}

// rollupDecrementSQL removes OLD from its bucket and drops emptied buckets.
// first_ts/last_ts are left as they were: exact counts, approximate edges.
func rollupDecrementSQL(t rollupTable) string {
	key := fmt.Sprintf("bucket = substr(OLD.timestamp, 1, %d) AND auth_index = OLD.auth_index AND auth_id = OLD.auth_id AND model = OLD.model", t.bucketLen)
	sets := []string{"requests = requests - 1"}
	for _, column := range rollupSumColumns {
		sets = append(sets, column+" = "+column+" - OLD."+column)
	}
	sets = append(sets, "pure_input_tokens = pure_input_tokens - ("+pureInputExpr("OLD.")+")")
	return "UPDATE " + t.name + " SET " + strings.Join(sets, ", ") + " WHERE " + key + ";\n" +
		"DELETE FROM " + t.name + " WHERE " + key + " AND requests <= 0"
}

func rollupBackfillSQL(t rollupTable) string {
	sums := make([]string, 0, len(rollupSumColumns))
	for _, column := range rollupSumColumns {
		sums = append(sums, "SUM("+column+")")
	}
	return "INSERT INTO " + t.name + " (bucket, auth_index, auth_id, model, source, provider, auth_type, requests, " +
		strings.Join(rollupSumColumns, ", ") + ", pure_input_tokens, first_ts, last_ts)\n" +
		fmt.Sprintf("SELECT substr(timestamp, 1, %d), auth_index, auth_id, model, MAX(source), MAX(provider), MAX(auth_type), COUNT(*), ", t.bucketLen) +
		strings.Join(sums, ", ") + ", SUM(" + pureInputSQL + "), MIN(timestamp), MAX(timestamp)\n" +
		"FROM usage_records GROUP BY 1, 2, 3, 4"
}

// ---- summary over rollups + details ----------------------------------------

// summaryPiece is one slice of a summary range, read from the details
// (timestamp bounds) or a rollup (bucket bounds). Empty bounds are open.
type summaryPiece struct {
	table    string
	from, to string
}

// planSummaryPieces splits [start, end) into whole days (daily rollup), the
// whole hours around them (hourly rollup) and the partial hours at the ends
// (details). Kept details answer those partial hours exactly. When a partial
// slice reaches before detailsSince (or no details are kept at all), the
// records retention deleted from that hour are added as a cleaned-hour piece:
// only that cleaned part snaps outward to the hour, and it is empty when
// nothing was ever cleaned, so ranges without retention stay exact.
func planSummaryPieces(start, end, detailsSince *time.Time) []summaryPiece {
	var pieces []summaryPiece
	detailsOrHour := func(from, to time.Time) {
		pieces = append(pieces, summaryPiece{rawSummaryKey, formatTimestamp(from), formatTimestamp(to)})
		if detailsSince == nil || from.Before(*detailsSince) {
			pieces = append(pieces, summaryPiece{table: cleanedHourKey, from: hourKey(from.Truncate(time.Hour))})
		}
	}
	rollup := func(t rollupTable, hasFrom bool, from time.Time, hasTo bool, to time.Time, key func(time.Time) string) {
		piece := summaryPiece{table: t.name}
		if hasFrom {
			piece.from = key(from)
		}
		if hasTo {
			piece.to = key(to)
		}
		pieces = append(pieces, piece)
	}

	hasStart, hasEnd := start != nil, end != nil
	var hourStart, hourEnd time.Time // the hour-aligned interior [hourStart, hourEnd)
	if hasStart {
		hourStart = ceilHour(start.UTC())
	}
	if hasEnd {
		hourEnd = end.UTC().Truncate(time.Hour)
	}
	if hasStart && hasEnd {
		if !start.Before(*end) {
			return nil
		}
		if hourStart.After(hourEnd) { // both ends inside one hour
			detailsOrHour(start.UTC(), end.UTC())
			return pieces
		}
	}
	if hasStart && !start.Equal(hourStart) {
		detailsOrHour(start.UTC(), hourStart)
	}
	if hasEnd && !end.Equal(hourEnd) {
		detailsOrHour(hourEnd, end.UTC())
	}

	var dayStart, dayEnd time.Time // whole days inside the interior
	if hasStart {
		dayStart = ceilDay(hourStart)
	}
	if hasEnd {
		dayEnd = floorDay(hourEnd)
	}
	if hasStart && hasEnd && !dayStart.Before(dayEnd) {
		if hourStart.Before(hourEnd) {
			rollup(hourlyRollup, true, hourStart, true, hourEnd, hourKey)
		}
		return pieces
	}
	if hasStart && hourStart.Before(dayStart) {
		rollup(hourlyRollup, true, hourStart, true, dayStart, hourKey)
	}
	rollup(dailyRollup, hasStart, dayStart, hasEnd, dayEnd, dayKey)
	if hasEnd && dayEnd.Before(hourEnd) {
		rollup(hourlyRollup, true, dayEnd, true, hourEnd, hourKey)
	}
	return pieces
}

func hourKey(t time.Time) string { return t.UTC().Format(hourBucketLayout) }
func dayKey(t time.Time) string  { return t.UTC().Format(dayBucketLayout) }

func ceilHour(t time.Time) time.Time {
	hour := t.Truncate(time.Hour)
	if hour.Before(t) {
		hour = hour.Add(time.Hour)
	}
	return hour
}

func floorDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func ceilDay(t time.Time) time.Time {
	day := floorDay(t)
	if day.Before(t) {
		day = day.AddDate(0, 0, 1)
	}
	return day
}

// summaryGroup is one (auth_index, auth_id, model) aggregate merged across pieces.
type summaryGroup struct {
	authIndex, authID, model   string
	source, provider, authType string
	requests, failed           int64
	tokens                     TokenStats
	pureInput                  int64
	first, last                string
}

type summaryGroupKey struct{ authIndex, authID, model string }

// summaryPieceSQL selects the same grouped columns from the details or a rollup.
func summaryPieceSQL(piece summaryPiece) (string, []any) {
	var selectCols, boundCol string
	sums := make([]string, 0, len(rollupSumColumns))
	for _, column := range rollupSumColumns {
		sums = append(sums, "SUM("+column+")")
	}
	if piece.table == rawSummaryKey {
		selectCols = "COUNT(*), " + strings.Join(sums, ", ") + ", SUM(" + pureInputSQL + "), MIN(timestamp), MAX(timestamp)"
		boundCol = "timestamp"
	} else {
		selectCols = "SUM(requests), " + strings.Join(sums, ", ") + ", SUM(pure_input_tokens), MIN(first_ts), MAX(last_ts)"
		boundCol = "bucket"
	}
	query := "SELECT auth_index, auth_id, model, MAX(source), MAX(provider), MAX(auth_type), " + selectCols + " FROM " + piece.table
	var where []string
	var args []any
	if piece.from != "" {
		where = append(where, boundCol+" >= ?")
		args = append(args, piece.from)
	}
	if piece.to != "" {
		where = append(where, boundCol+" < ?")
		args = append(args, piece.to)
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	return query + " GROUP BY auth_index, auth_id, model", args
}

func scanSummaryPiece(ctx context.Context, tx *sql.Tx, piece summaryPiece, groups map[summaryGroupKey]*summaryGroup) error {
	if piece.table == cleanedHourKey {
		return scanCleanedHour(ctx, tx, piece.from, groups)
	}
	query, args := summaryPieceSQL(piece)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("usage sqlite summary %s: %w", piece.table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var g summaryGroup
		if err := rows.Scan(&g.authIndex, &g.authID, &g.model, &g.source, &g.provider, &g.authType,
			&g.requests, &g.failed,
			&g.tokens.InputTokens, &g.tokens.OutputTokens, &g.tokens.ReasoningTokens, &g.tokens.CachedTokens,
			&g.tokens.CacheReadTokens, &g.tokens.CacheCreationTokens, &g.tokens.TotalTokens,
			&g.pureInput, &g.first, &g.last); err != nil {
			return fmt.Errorf("usage sqlite summary scan: %w", err)
		}
		mergeSummaryGroup(groups, &g)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("usage sqlite summary rows: %w", err)
	}
	return nil
}

// scanCleanedHour adds the records of hour that retention deleted: the hour's
// rollup minus the details still kept for it. Deleted records all precede the
// kept ones, so the bucket's first_ts is theirs; their last time is unknown
// and first_ts stands in for it.
func scanCleanedHour(ctx context.Context, tx *sql.Tx, hour string, groups map[summaryGroupKey]*summaryGroup) error {
	start, err := time.Parse(hourBucketLayout, hour)
	if err != nil {
		return fmt.Errorf("usage sqlite cleaned hour %q: %w", hour, err)
	}
	end := start.Add(time.Hour)
	rolled := make(map[summaryGroupKey]*summaryGroup)
	kept := make(map[summaryGroupKey]*summaryGroup)
	if err := scanSummaryPiece(ctx, tx, summaryPiece{hourlyRollup.name, hourKey(start), hourKey(end)}, rolled); err != nil {
		return err
	}
	if err := scanSummaryPiece(ctx, tx, summaryPiece{rawSummaryKey, formatTimestamp(start), formatTimestamp(end)}, kept); err != nil {
		return err
	}
	for key, cleaned := range rolled {
		if k, ok := kept[key]; ok {
			cleaned.requests -= k.requests
			cleaned.failed -= k.failed
			cleaned.pureInput -= k.pureInput
			cleaned.tokens = TokenStats{
				InputTokens:         cleaned.tokens.InputTokens - k.tokens.InputTokens,
				OutputTokens:        cleaned.tokens.OutputTokens - k.tokens.OutputTokens,
				ReasoningTokens:     cleaned.tokens.ReasoningTokens - k.tokens.ReasoningTokens,
				CachedTokens:        cleaned.tokens.CachedTokens - k.tokens.CachedTokens,
				CacheReadTokens:     cleaned.tokens.CacheReadTokens - k.tokens.CacheReadTokens,
				CacheCreationTokens: cleaned.tokens.CacheCreationTokens - k.tokens.CacheCreationTokens,
				TotalTokens:         cleaned.tokens.TotalTokens - k.tokens.TotalTokens,
			}
			cleaned.last = cleaned.first
		}
		if cleaned.requests > 0 {
			mergeSummaryGroup(groups, cleaned)
		}
	}
	return nil
}

// mergeSummaryGroup folds g into groups with the summary's MAX/SUM/MIN rules.
func mergeSummaryGroup(groups map[summaryGroupKey]*summaryGroup, g *summaryGroup) {
	key := summaryGroupKey{g.authIndex, g.authID, g.model}
	existing, ok := groups[key]
	if !ok {
		groups[key] = g
		return
	}
	existing.source = max(existing.source, g.source)
	existing.provider = max(existing.provider, g.provider)
	existing.authType = max(existing.authType, g.authType)
	existing.requests += g.requests
	existing.failed += g.failed
	addTokens(&existing.tokens, g.tokens)
	existing.pureInput += g.pureInput
	existing.first = min(existing.first, g.first)
	existing.last = max(existing.last, g.last)
}

// detailsSince returns the earliest retained record, or nil when there is none.
func detailsSince(ctx context.Context, tx *sql.Tx) (*time.Time, error) {
	var text sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT MIN(timestamp) FROM usage_records`).Scan(&text); err != nil {
		return nil, fmt.Errorf("usage sqlite details since: %w", err)
	}
	if !text.Valid {
		return nil, nil
	}
	parsed, err := parseStoredTimestamp(text.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
