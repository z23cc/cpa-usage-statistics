package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Muse (provider "meta") auto-disable. CPA already swaps the key once inside
// the failing request (dca_token → new API key) and then keeps a rejected
// credential out of rotation in memory only. This worker finishes the job:
// two minutes after a Muse 401 it swaps the key itself once more, and only
// disables the credential file when Meta refuses the swap or the new key is
// rejected too. Network errors and 5xx never disable.
//
// account_health is the only state. usage.handle (observeUsage) and the
// worker both write it: ingestion through single SQL statements, the worker
// only when the row's version is still the one it decided on, so a 401 or a
// success arriving while the worker talks to the host is never lost.

const (
	healthCheckDelay       = 2 * time.Minute
	healthTickInterval     = 15 * time.Second
	healthHostRetryDelay   = time.Minute
	mintedWatchWindow      = 24 * time.Hour
	disabledReconcileEvery = 5 * time.Minute
	healthMessageLimit     = 300
	healthDueBatch         = 50
	backfillWindow         = 7 * 24 * time.Hour
	backfillMetaKey        = "health_backfill_v1"
	lastFailureWindow      = 24 * time.Hour
	mintTimeout            = 30 * time.Second

	disabledByMarker   = pluginID
	metaMintDefaultURL = "https://api.meta.ai/muse-code/key"
	// Keep in sync with CPA internal/auth/meta MintAPIKey.
	metaMintUserAgent = "muse-code/1.0.2"
)

var mintRetryDelays = []time.Duration{5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour}

// markerFields are what markDisabled adds to a credential file.
var markerFields = []string{"disabled_by", "disabled_reason", "disabled_at"}

const (
	healthPending  = "pending"
	healthRetry    = "retry"
	healthMinted   = "minted"
	healthDisabled = "disabled"

	reasonMintRejected   = "mint_rejected"
	reasonNewKeyRejected = "new_key_rejected"
	reasonNoDCAToken     = "no_dca_token"
	reasonBackfill       = "backfill_401"
)

// ---- schema -----------------------------------------------------------------

// account_last_failure is kept by a trigger like the rollups, so the hot
// insert path gains no Go code and an older plugin version keeps it current.
const lastFailureTrigger = `CREATE TRIGGER IF NOT EXISTS usage_records_last_failure_ai AFTER INSERT ON usage_records
WHEN NEW.failed = 1 AND NEW.auth_id != '' BEGIN
INSERT INTO account_last_failure (auth_id, auth_index, status_code, message, at)
VALUES (NEW.auth_id, NEW.auth_index, NEW.failure_status_code, substr(NEW.failure_body, 1, 300), NEW.timestamp)
ON CONFLICT (auth_id) DO UPDATE SET auth_index = excluded.auth_index, status_code = excluded.status_code,
	message = excluded.message, at = excluded.at
WHERE excluded.at >= account_last_failure.at;
END`

// initHealthSchema only adds tables and a trigger; it never touches
// PRAGMA user_version, which guards the rollups.
func (s *SQLiteStore) initHealthSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS account_health (
	auth_id TEXT PRIMARY KEY,
	auth_index TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	http_status INTEGER NOT NULL DEFAULT 0,
	message TEXT NOT NULL DEFAULT '',
	first_401_at TEXT NOT NULL DEFAULT '',
	last_401_at TEXT NOT NULL DEFAULT '',
	check_at TEXT NOT NULL DEFAULT '',
	minted_at TEXT NOT NULL DEFAULT '',
	key_fp TEXT NOT NULL DEFAULT '',
	attempts INTEGER NOT NULL DEFAULT 0,
	version INTEGER NOT NULL DEFAULT 0,
	updated_at TEXT NOT NULL
) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS account_last_failure (
	auth_id TEXT PRIMARY KEY,
	auth_index TEXT NOT NULL DEFAULT '',
	status_code INTEGER NOT NULL DEFAULT 0,
	message TEXT NOT NULL DEFAULT '',
	at TEXT NOT NULL
) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS plugin_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '') WITHOUT ROWID`,
		lastFailureTrigger,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("usage sqlite init health schema: %w", err)
		}
	}
	return nil
}

// ---- rows -------------------------------------------------------------------

type healthRow struct {
	AuthID     string
	AuthIndex  string
	State      string
	Reason     string
	HTTPStatus int
	Message    string
	First401At time.Time
	// Last401At is when the latest 401's request ended (see recordEnd).
	Last401At time.Time
	CheckAt   time.Time
	MintedAt  time.Time
	KeyFP     string
	Attempts  int
	// Version grows with every write; worker writes require it unchanged.
	Version   int64
	UpdatedAt time.Time
}

const healthColumns = `auth_id, auth_index, state, reason, http_status, message, first_401_at, last_401_at, check_at, minted_at, key_fp, attempts, version, updated_at`

func optionalTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return formatTimestamp(t)
}

func parseOptionalTimestamp(text string) time.Time {
	if strings.TrimSpace(text) == "" {
		return time.Time{}
	}
	t, err := parseStoredTimestamp(text)
	if err != nil {
		return time.Time{}
	}
	return t
}

func scanHealthRows(rows *sql.Rows) ([]healthRow, error) {
	defer func() { _ = rows.Close() }()
	var out []healthRow
	for rows.Next() {
		var (
			r                                       healthRow
			first, last, check, minted, updatedText string
		)
		if err := rows.Scan(&r.AuthID, &r.AuthIndex, &r.State, &r.Reason, &r.HTTPStatus, &r.Message,
			&first, &last, &check, &minted, &r.KeyFP, &r.Attempts, &r.Version, &updatedText); err != nil {
			return nil, fmt.Errorf("usage sqlite scan health: %w", err)
		}
		r.First401At, r.Last401At = parseOptionalTimestamp(first), parseOptionalTimestamp(last)
		r.CheckAt, r.MintedAt = parseOptionalTimestamp(check), parseOptionalTimestamp(minted)
		r.UpdatedAt = parseOptionalTimestamp(updatedText)
		out = append(out, r)
	}
	return out, rows.Err()
}

func affected(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// observe401 opens (or extends) the check for a credential. started and ended
// bound the 401's request. A later 401 only ever moves check_at earlier; on a
// disabled row it wakes the worker, which restarts the check if the file was
// enabled again. A request started before this plugin's swap carried the
// replaced key: CPA drops its result as stale, and so does this.
func (s *SQLiteStore) observe401(ctx context.Context, authID, authIndex, body string, started, ended, checkAt, now time.Time) error {
	at := formatTimestamp(ended)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO account_health (auth_id, auth_index, state, http_status, message, first_401_at, last_401_at, check_at, version, updated_at)
VALUES (?, ?, 'pending', 401, ?, ?, ?, ?, 1, ?)
ON CONFLICT (auth_id) DO UPDATE SET
	auth_index = CASE WHEN excluded.auth_index != '' THEN excluded.auth_index ELSE account_health.auth_index END,
	last_401_at = max(account_health.last_401_at, excluded.last_401_at),
	check_at = CASE WHEN account_health.check_at = '' OR excluded.check_at < account_health.check_at
		THEN excluded.check_at ELSE account_health.check_at END,
	message = CASE WHEN account_health.state IN ('pending', 'minted') THEN excluded.message ELSE account_health.message END,
	version = account_health.version + 1,
	updated_at = excluded.updated_at
WHERE NOT (account_health.state = 'minted' AND ? < account_health.minted_at)`,
		authID, authIndex, trimMessage(body), at, at, formatTimestamp(checkAt), formatTimestamp(now), formatTimestamp(started))
	if err != nil {
		return fmt.Errorf("usage sqlite observe 401: %w", err)
	}
	return nil
}

// clearOnSuccess ends the check when a request started after the latest 401
// ended (and after this plugin's swap) succeeded. An older stream finishing
// late proves nothing. Disabled rows stay.
func (s *SQLiteStore) clearOnSuccess(ctx context.Context, authID string, started time.Time) (bool, error) {
	at := formatTimestamp(started)
	return affected(s.db.ExecContext(ctx, `DELETE FROM account_health
WHERE auth_id = ? AND state != 'disabled' AND last_401_at <= ? AND (state != 'minted' OR minted_at <= ?)`, authID, at, at))
}

// updateHealthIf writes the worker-owned columns if nobody wrote the row since
// it was read. last_401_at and auth_index stay owned by observe401.
func (s *SQLiteStore) updateHealthIf(ctx context.Context, r healthRow) (bool, error) {
	return affected(s.db.ExecContext(ctx, `UPDATE account_health SET state = ?, reason = ?, http_status = ?, message = ?,
	first_401_at = ?, check_at = ?, minted_at = ?, key_fp = ?, attempts = ?, version = version + 1, updated_at = ?
WHERE auth_id = ? AND version = ?`,
		r.State, r.Reason, r.HTTPStatus, trimMessage(r.Message), optionalTimestamp(r.First401At), optionalTimestamp(r.CheckAt),
		optionalTimestamp(r.MintedAt), r.KeyFP, r.Attempts, formatTimestamp(r.UpdatedAt), r.AuthID, r.Version))
}

// saveDisabled records a credential this plugin disabled (or adopted). The
// file is already disabled, so this overwrites whatever the row says now.
func (s *SQLiteStore) saveDisabled(ctx context.Context, r healthRow) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO account_health (`+healthColumns+`) VALUES (?, ?, 'disabled', ?, ?, ?, ?, ?, '', '', '', 0, 1, ?)
ON CONFLICT (auth_id) DO UPDATE SET state = 'disabled', reason = excluded.reason, http_status = excluded.http_status,
	message = excluded.message, check_at = '', attempts = 0, version = account_health.version + 1, updated_at = excluded.updated_at`,
		r.AuthID, r.AuthIndex, r.Reason, r.HTTPStatus, trimMessage(r.Message),
		optionalTimestamp(r.First401At), optionalTimestamp(r.Last401At), formatTimestamp(r.UpdatedAt))
	if err != nil {
		return fmt.Errorf("usage sqlite save disabled: %w", err)
	}
	return nil
}

// insertHealth adds a row unless one exists (backfill must not override a
// check that live traffic already opened).
func (s *SQLiteStore) insertHealth(ctx context.Context, r healthRow) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO account_health (`+healthColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
		r.AuthID, r.AuthIndex, r.State, r.Reason, r.HTTPStatus, trimMessage(r.Message),
		optionalTimestamp(r.First401At), optionalTimestamp(r.Last401At), optionalTimestamp(r.CheckAt),
		optionalTimestamp(r.MintedAt), r.KeyFP, r.Attempts, formatTimestamp(r.UpdatedAt))
	if err != nil {
		return fmt.Errorf("usage sqlite insert health: %w", err)
	}
	return nil
}

// installHealth records a swap that is already on disk. Any 401 recorded so
// far was sent with the replaced key (the new one was saved a moment ago), so
// last_401_at is capped at minted_at: only a later 401 can reject the new key.
func (s *SQLiteStore) installHealth(ctx context.Context, authID, keyFP string, mintedAt time.Time) (bool, error) {
	at := formatTimestamp(mintedAt)
	return affected(s.db.ExecContext(ctx, `UPDATE account_health SET state = 'minted', reason = '', http_status = 0, message = '',
	attempts = 0, minted_at = ?, key_fp = ?, last_401_at = min(last_401_at, ?), check_at = ?, version = version + 1, updated_at = ?
WHERE auth_id = ?`, at, keyFP, at, formatTimestamp(mintedAt.Add(mintedWatchWindow)), at, authID))
}

func (s *SQLiteStore) deleteHealthIf(ctx context.Context, authID string, version int64) (bool, error) {
	return affected(s.db.ExecContext(ctx, `DELETE FROM account_health WHERE auth_id = ? AND version = ?`, authID, version))
}

// healthVersion reads a row's version on the writer, so it sees every commit.
func (s *SQLiteStore) healthVersion(ctx context.Context, authID string) (int64, bool, error) {
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT version FROM account_health WHERE auth_id = ?`, authID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("usage sqlite read health version: %w", err)
	}
	return version, true, nil
}

func (s *SQLiteStore) healthRowsWhere(ctx context.Context, where string, args ...any) ([]healthRow, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT `+healthColumns+` FROM account_health `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("usage sqlite query health: %w", err)
	}
	return scanHealthRows(rows)
}

// dueHealthRows also returns disabled rows a 401 woke up (check_at set).
func (s *SQLiteStore) dueHealthRows(ctx context.Context, now time.Time) ([]healthRow, error) {
	return s.healthRowsWhere(ctx, `WHERE check_at != '' AND check_at <= ? ORDER BY check_at LIMIT ?`,
		formatTimestamp(now), healthDueBatch)
}

func (s *SQLiteStore) healthIDs(ctx context.Context) ([]string, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT auth_id FROM account_health`)
	if err != nil {
		return nil, fmt.Errorf("usage sqlite query health ids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("usage sqlite scan health id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *SQLiteStore) metaFlag(ctx context.Context, key string) (bool, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plugin_meta WHERE key = ?`, key).Scan(&n); err != nil {
		return false, fmt.Errorf("usage sqlite read meta: %w", err)
	}
	return n > 0, nil
}

func (s *SQLiteStore) setMetaFlag(ctx context.Context, key, value string) error {
	if _, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO plugin_meta (key, value) VALUES (?, ?)`, key, value); err != nil {
		return fmt.Errorf("usage sqlite write meta: %w", err)
	}
	return nil
}

type backfillCandidate struct {
	AuthID, AuthIndex, Body string
	// At is when the 401's request ended (see recordEnd).
	At time.Time
}

// backfillCandidates lists Muse credentials whose latest record since since is
// a 401 (SQLite returns the bare columns of the MAX(timestamp) row). It scans
// a week of records, so it runs on the read pool, never blocking inserts.
func (s *SQLiteStore) backfillCandidates(ctx context.Context, since time.Time) ([]backfillCandidate, error) {
	rows, err := s.rdb.QueryContext(ctx, `
SELECT auth_id, auth_index, failed, failure_status_code, failure_body, latency_ms, MAX(timestamp)
FROM usage_records WHERE timestamp >= ? AND lower(provider) = 'meta' AND auth_id != ''
GROUP BY auth_id`, formatTimestamp(since))
	if err != nil {
		return nil, fmt.Errorf("usage sqlite backfill candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []backfillCandidate
	for rows.Next() {
		var (
			c          backfillCandidate
			failed     int
			status     int
			latencyMs  int64
			latestText string
		)
		if err := rows.Scan(&c.AuthID, &c.AuthIndex, &failed, &status, &c.Body, &latencyMs, &latestText); err != nil {
			return nil, fmt.Errorf("usage sqlite backfill scan: %w", err)
		}
		if failed != 1 || status != http.StatusUnauthorized {
			continue
		}
		c.At = parseOptionalTimestamp(latestText).Add(time.Duration(latencyMs) * time.Millisecond)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---- usage.handle fast path ----------------------------------------------------

// healthSeen holds every auth_id that has (or had) a row, so a success record
// costs one map lookup unless its credential was ever checked. Entries are
// never removed while the worker runs: a stale one only costs a no-op DELETE.
type healthSeenSet struct {
	mu  sync.Mutex
	ids map[string]struct{}
}

func (s *healthSeenSet) add(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids == nil {
		s.ids = make(map[string]struct{})
	}
	s.ids[id] = struct{}{}
}

func (s *healthSeenSet) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.ids[id]
	return ok
}

func (s *healthSeenSet) reset(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = make(map[string]struct{}, len(ids))
	for _, id := range ids {
		s.ids[id] = struct{}{}
	}
}

// ---- worker lifecycle ---------------------------------------------------------

var (
	metaAutoDisable atomic.Bool
	// healthRunning gates the usage.handle hook; true only while the worker runs.
	healthRunning atomic.Bool
	healthSeen    healthSeenSet
	// healthNow and testHealthHost are overridden by tests.
	healthNow      = time.Now
	testHealthHost pluginHost

	healthMu     sync.Mutex
	healthCancel context.CancelFunc
	healthDone   chan struct{}
)

func currentPluginHost() pluginHost {
	if testHealthHost != nil {
		return testHealthHost
	}
	if hostCallFn != nil {
		return rpcHost{call: hostCallFn}
	}
	return nil
}

type healthWorker struct {
	store *SQLiteStore
	host  pluginHost
}

// startHealth (re)starts the worker when meta_auto_disable is on and a host
// and store exist. Called after ensureStore, like startCleanup.
func startHealth() {
	stopHealth()
	if !metaAutoDisable.Load() {
		return
	}
	store, host := currentStore(), currentPluginHost()
	if store == nil || host == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	healthMu.Lock()
	healthCancel, healthDone = cancel, done
	healthMu.Unlock()
	w := &healthWorker{store: store, host: host}
	// Everything that may call the host runs here, never inside the host's
	// plugin.register / reconfigure call.
	go func() {
		defer close(done)
		w.run(ctx)
	}()
}

// stopHealth cancels the worker and waits for it (an in-flight swap is
// cancelled through host.http.cancel), so it never outlives the store.
func stopHealth() {
	healthRunning.Store(false)
	healthMu.Lock()
	cancel, done := healthCancel, healthDone
	healthCancel, healthDone = nil, nil
	healthMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	// Again after the wait: the worker may have switched it on meanwhile.
	healthRunning.Store(false)
	healthSeen.reset(nil)
}

func (w *healthWorker) run(ctx context.Context) {
	ids, err := w.store.healthIDs(ctx)
	if err != nil {
		w.host.log("warn", "auto-disable not started: "+err.Error(), nil)
		return
	}
	healthSeen.reset(ids)
	healthRunning.Store(true)
	ticker := time.NewTicker(healthTickInterval)
	defer ticker.Stop()
	backfilled, warned := false, false
	var nextReconcile time.Time
	for {
		// On a cold CPA start the plugin is registered before the auth manager
		// exists, so the backfill keeps retrying until it gets through.
		if !backfilled {
			if err := w.backfill(ctx); err == nil {
				backfilled = true
			} else if ctx.Err() == nil && !warned {
				w.host.log("warn", "auto-disable backfill will retry: "+err.Error(), nil)
				warned = true
			}
		}
		now := healthNow()
		w.processDue(ctx, now)
		if !now.Before(nextReconcile) {
			w.reconcileDisabled(ctx)
			nextReconcile = now.Add(disabledReconcileEvery)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ---- usage.handle hook --------------------------------------------------------

// observeUsage runs after a record is stored. Errors are swallowed: the
// record itself is already safe and the check is best effort.
func observeUsage(ctx context.Context, store *SQLiteStore, rec Record, now time.Time) {
	authID := strings.TrimSpace(rec.AuthID)
	if authID == "" || store == nil {
		return
	}
	if rec.FailureStatusCode == http.StatusUnauthorized && strings.EqualFold(strings.TrimSpace(rec.Provider), "meta") {
		if err := store.observe401(ctx, authID, strings.TrimSpace(rec.AuthIndex), rec.FailureBody,
			rec.Timestamp, recordEnd(rec, now), now.Add(healthCheckDelay), now); err == nil {
			healthSeen.add(authID)
		}
		return
	}
	if !rec.Failed && healthSeen.has(authID) {
		_, _ = store.clearOnSuccess(ctx, authID, rec.Timestamp)
	}
}

// recordEnd is when the request behind a usage record ended: its start
// (RequestedAt, per upstream attempt) plus its latency. Falls back to the
// arrival time for records without a start.
func recordEnd(rec Record, arrived time.Time) time.Time {
	if rec.Timestamp.IsZero() {
		return arrived
	}
	return rec.Timestamp.Add(time.Duration(rec.LatencyMs) * time.Millisecond)
}

// ---- state machine ------------------------------------------------------------

func (w *healthWorker) processDue(ctx context.Context, now time.Time) {
	rows, err := w.store.dueHealthRows(ctx, now)
	if err != nil {
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		w.process(ctx, row)
	}
}

func (w *healthWorker) process(ctx context.Context, row healthRow) {
	if strings.TrimSpace(row.AuthIndex) == "" {
		w.drop(ctx, row, "no auth_index")
		return
	}
	rt, err := w.host.authRuntime(row.AuthIndex)
	if errors.Is(err, errHostAuthNotFound) {
		w.drop(ctx, row, "credential gone")
		return
	}
	if err != nil {
		w.postpone(ctx, row)
		return
	}
	switch {
	case row.State == healthDisabled && rt.Disabled:
		// Woken by a 401 that was in flight when it was disabled: stay asleep.
		row.CheckAt, row.UpdatedAt = time.Time{}, healthNow()
		_, _ = w.store.updateHealthIf(ctx, row)
		return
	case row.State == healthDisabled:
		// Enabled again (re-login or admin) and rejected again: start over.
		if !w.stripMarker(row.AuthIndex) {
			w.postpone(ctx, row)
			return
		}
		row.State, row.Reason, row.HTTPStatus, row.Attempts, row.First401At = healthPending, "", http.StatusUnauthorized, 0, row.Last401At
	case rt.Disabled:
		w.drop(ctx, row, "disabled by someone else")
		return
	}
	// Recovered as far as CPA can tell: it swapped the key after the latest 401
	// ended, and that key is not proven bad yet (a 401 on it starts a new
	// round). Last401At is when that 401's request ended, not when its record
	// arrived, so a record delivered late cannot hide a swap; CPA's
	// "swap, retry, 401 again" stays unrecovered because the retry ends after
	// the swap. The raw runtime status is not used: CPA keeps it "active"
	// while the rejected key still blocks the credential (rehearsal on v8.0.38).
	recovered := rt.LastRefresh.After(row.Last401At)
	why := fmt.Sprintf("recovered in CPA (last_refresh=%s last_401=%s)",
		rt.LastRefresh.UTC().Format(time.RFC3339Nano), row.Last401At.UTC().Format(time.RFC3339Nano))
	if row.State == healthMinted {
		switch {
		case !row.Last401At.After(row.MintedAt):
			w.drop(ctx, row, "new key not rejected within the watch window")
		case recovered:
			w.drop(ctx, row, why)
		default:
			// Our key was rejected, and so was any key CPA swapped in since.
			w.disable(ctx, row, reasonNewKeyRejected, http.StatusUnauthorized, row.Message, row.KeyFP)
		}
		return
	}
	if recovered {
		w.drop(ctx, row, why)
		return
	}
	w.swap(ctx, row)
}

func (w *healthWorker) swap(ctx context.Context, row healthRow) {
	file, doc, err := w.readFile(row.AuthIndex)
	if errors.Is(err, errHostAuthNotFound) {
		w.drop(ctx, row, "credential gone")
		return
	}
	if err != nil {
		w.postpone(ctx, row)
		return
	}
	fp := credentialFingerprint(doc)
	dca := dcaToken(doc)
	if dca == "" {
		w.disable(ctx, row, reasonNoDCAToken, 0, "", fp)
		return
	}
	mintCtx, cancel := context.WithTimeout(ctx, mintTimeout)
	resp, errMint := w.host.httpDo(mintCtx, mintRequest(dca))
	cancel()
	if ctx.Err() != nil {
		return
	}
	switch {
	case errMint != nil:
		w.retry(ctx, row, 0, errMint.Error())
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		w.disable(ctx, row, reasonMintRejected, resp.StatusCode, string(resp.Body), fp)
	case resp.StatusCode < 200 || resp.StatusCode > 299:
		w.retry(ctx, row, resp.StatusCode, string(resp.Body))
	default:
		var minted mintedKey
		if json.Unmarshal(resp.Body, &minted) != nil || strings.TrimSpace(minted.APIKey) == "" {
			w.retry(ctx, row, resp.StatusCode, "mint response has no api_key")
			return
		}
		w.install(ctx, row, file.Name, fp, minted)
	}
}

// install writes the minted key back, unless the file changed since it was
// read (e.g. the supplier logged in again meanwhile), it was disabled
// meanwhile, or the credential recovered meanwhile (its row is gone).
func (w *healthWorker) install(ctx context.Context, row healthRow, name, fp string, minted mintedKey) {
	_, doc, err := w.readFile(row.AuthIndex)
	if err != nil {
		w.postpone(ctx, row)
		return
	}
	switch {
	case credentialFingerprint(doc) != fp:
		w.drop(ctx, row, "credential changed meanwhile")
		return
	case docDisabled(doc):
		w.drop(ctx, row, "disabled by someone else")
		return
	}
	if _, exists, err := w.store.healthVersion(ctx, row.AuthID); err != nil || !exists {
		return
	}
	applyMintedKey(doc, minted, healthNow())
	data, err := json.Marshal(doc)
	if err == nil {
		err = w.host.authSave(name, data)
	}
	if err != nil {
		w.retry(ctx, row, 0, "save minted key: "+err.Error())
		return
	}
	// The new key clears CPA's rejected-token block, but not the 30-minute
	// model cooldown the 401 left behind (seen in the rehearsal on v8.0.38):
	// without this the account would sit idle until it expires. Best effort.
	_ = w.host.resetCooldown(row.AuthIndex)
	// Stamped after the save: requests started before it carried the old key.
	if ok, _ := w.store.installHealth(ctx, row.AuthID, credentialFingerprint(doc), healthNow()); ok {
		w.host.log("info", "swapped the Muse key after a 401", map[string]any{"auth_id": row.AuthID})
	}
}

// disable marks the credential file disabled with this plugin's marker.
// wantFP is the fingerprint the decision was made on; a changed file means
// someone re-logged in, so nothing is disabled. A file someone else disabled
// meanwhile is left as their (manual) disable.
func (w *healthWorker) disable(ctx context.Context, row healthRow, reason string, status int, message, wantFP string) {
	file, doc, err := w.readFile(row.AuthIndex)
	if errors.Is(err, errHostAuthNotFound) {
		w.drop(ctx, row, "credential gone")
		return
	}
	if err != nil {
		w.postpone(ctx, row)
		return
	}
	if wantFP != "" && credentialFingerprint(doc) != wantFP {
		w.drop(ctx, row, "credential changed meanwhile")
		return
	}
	if docDisabled(doc) {
		if stringField(doc, "disabled_by") != disabledByMarker {
			w.drop(ctx, row, "disabled by someone else")
			return
		}
	} else {
		// A success or another 401 while the worker was busy reopens the case.
		version, exists, err := w.store.healthVersion(ctx, row.AuthID)
		if err != nil || !exists || version != row.Version {
			return
		}
		markDisabled(doc, reason, healthNow())
		data, err := json.Marshal(doc)
		if err == nil {
			err = w.host.authSave(file.Name, data)
		}
		if err != nil {
			w.postpone(ctx, row)
			return
		}
	}
	row.Reason, row.HTTPStatus, row.Message, row.UpdatedAt = reason, status, message, healthNow()
	_ = w.store.saveDisabled(ctx, row)
	healthSeen.add(row.AuthID)
	w.host.log("warn", "disabled a Muse credential after a 401", map[string]any{
		"auth_id": row.AuthID, "reason": reason, "http_status": status,
	})
}

func (w *healthWorker) retry(ctx context.Context, row healthRow, status int, message string) {
	now := healthNow()
	row.Attempts++
	delay := mintRetryDelays[min(row.Attempts, len(mintRetryDelays))-1]
	row.State, row.HTTPStatus, row.Message = healthRetry, status, message
	row.CheckAt, row.UpdatedAt = now.Add(delay), now
	if ok, _ := w.store.updateHealthIf(ctx, row); ok {
		w.host.log("warn", "Muse key swap failed, will retry", map[string]any{
			"auth_id": row.AuthID, "http_status": status, "retry_in": delay.String(),
		})
	}
}

// postpone retries a row whose host call failed, without counting a swap attempt.
func (w *healthWorker) postpone(ctx context.Context, row healthRow) {
	now := healthNow()
	row.CheckAt, row.UpdatedAt = now.Add(healthHostRetryDelay), now
	_, _ = w.store.updateHealthIf(ctx, row)
}

// drop ends the check unless the row changed meanwhile (then it is due again).
func (w *healthWorker) drop(ctx context.Context, row healthRow, why string) {
	if ok, _ := w.store.deleteHealthIf(ctx, row.AuthID, row.Version); ok {
		w.host.log("info", "stopped tracking a Muse credential: "+why, map[string]any{"auth_id": row.AuthID})
	}
}

// reconcileDisabled ends disabled rows whose file is gone or enabled again
// (re-login, admin enable), first stripping this plugin's marker from an
// enabled file so a later manual disable is never mistaken for an automatic one.
func (w *healthWorker) reconcileDisabled(ctx context.Context) {
	rows, err := w.store.healthRowsWhere(ctx, `WHERE state = 'disabled'`)
	if err != nil {
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		rt, err := w.host.authRuntime(row.AuthIndex)
		switch {
		case errors.Is(err, errHostAuthNotFound):
			w.drop(ctx, row, "credential gone")
		case err == nil && !rt.Disabled && w.stripMarker(row.AuthIndex):
			w.drop(ctx, row, "no longer disabled")
		}
	}
}

// stripMarker removes this plugin's marker from an enabled credential file.
// It reports whether the file is clean (or gone) afterwards.
func (w *healthWorker) stripMarker(authIndex string) bool {
	file, doc, err := w.readFile(authIndex)
	if errors.Is(err, errHostAuthNotFound) {
		return true
	}
	if err != nil || docDisabled(doc) {
		return false
	}
	changed := false
	for _, field := range markerFields {
		if _, ok := doc[field]; ok {
			delete(doc, field)
			changed = true
		}
	}
	if !changed {
		return true
	}
	data, err := json.Marshal(doc)
	return err == nil && w.host.authSave(file.Name, data) == nil
}

// backfill runs once per database: credentials whose last record in the past
// week is a Muse 401 get the marker if already disabled (so a re-login revives
// them), or a check now if not. It returns an error until it got through.
func (w *healthWorker) backfill(ctx context.Context) error {
	done, err := w.store.metaFlag(ctx, backfillMetaKey)
	if err != nil || done {
		return err
	}
	now := healthNow()
	candidates, err := w.store.backfillCandidates(ctx, now.Add(-backfillWindow))
	if err != nil {
		return err
	}
	marked, queued := 0, 0
	for _, c := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rt, err := w.host.authRuntime(c.AuthIndex)
		if errors.Is(err, errHostAuthNotFound) {
			continue
		}
		if err != nil {
			// The host is not ready (cold start): retry the whole backfill.
			return err
		}
		// A credential that fails on its own is logged and skipped, so one bad
		// file cannot make the whole backfill (and its re-queued rows) repeat.
		switch done, err := w.backfillOne(ctx, c, rt, now); {
		case err != nil:
			w.host.log("warn", "auto-disable backfill skipped a credential: "+err.Error(), map[string]any{"auth_id": c.AuthID})
		case done == healthPending:
			queued++
		case done == healthDisabled:
			marked++
		}
	}
	if err := w.store.setMetaFlag(ctx, backfillMetaKey, formatTimestamp(now)); err != nil {
		return err
	}
	w.host.log("info", "auto-disable backfill done", map[string]any{"marked": marked, "queued": queued})
	return nil
}

// backfillOne queues an enabled candidate or adopts a disabled one; it returns
// the state it recorded ("" when the credential is not ours to adopt).
func (w *healthWorker) backfillOne(ctx context.Context, c backfillCandidate, rt hostAuthRuntime, now time.Time) (string, error) {
	row := healthRow{AuthID: c.AuthID, AuthIndex: c.AuthIndex, HTTPStatus: http.StatusUnauthorized,
		Message: c.Body, First401At: c.At, Last401At: c.At, UpdatedAt: now}
	if !rt.Disabled {
		row.State, row.CheckAt = healthPending, now
		if err := w.store.insertHealth(ctx, row); err != nil {
			return "", err
		}
		healthSeen.add(c.AuthID)
		return healthPending, nil
	}
	file, doc, err := w.readFile(c.AuthIndex)
	if err != nil {
		return "", err
	}
	switch marker := stringField(doc, "disabled_by"); {
	case !docDisabled(doc) || (marker != "" && marker != disabledByMarker):
		return "", nil
	case marker == "":
		markDisabled(doc, reasonBackfill, now)
		data, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		if err := w.host.authSave(file.Name, data); err != nil {
			return "", err
		}
	}
	row.Reason = reasonBackfill
	if err := w.store.saveDisabled(ctx, row); err != nil {
		return "", err
	}
	healthSeen.add(c.AuthID)
	return healthDisabled, nil
}

// ---- credential file ----------------------------------------------------------

func (w *healthWorker) readFile(authIndex string) (hostAuthFile, map[string]any, error) {
	file, err := w.host.authGet(authIndex)
	if err != nil {
		return hostAuthFile{}, nil, err
	}
	if strings.TrimSpace(file.Name) == "" {
		return hostAuthFile{}, nil, fmt.Errorf("auth file name missing for auth_index %s", authIndex)
	}
	decoder := json.NewDecoder(bytes.NewReader(file.JSON))
	decoder.UseNumber() // keep integers (e.g. dca_expires_at) exactly as written
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil || doc == nil {
		return hostAuthFile{}, nil, fmt.Errorf("auth file is not a JSON object: %v", err)
	}
	return file, doc, nil
}

func stringField(doc map[string]any, key string) string {
	value, _ := doc[key].(string)
	return strings.TrimSpace(value)
}

func docDisabled(doc map[string]any) bool {
	disabled, _ := doc["disabled"].(bool)
	return disabled
}

// dcaToken mirrors CPA's extractDCAToken for file credentials.
func dcaToken(doc map[string]any) string {
	if token := stringField(doc, "dca_token"); token != "" {
		return token
	}
	if token := stringField(doc, "access_token"); strings.HasPrefix(token, "dca:") {
		return token
	}
	return ""
}

// credentialFingerprint identifies the login behind a file without keeping
// any of it. Only the dca token counts: a re-login brings a new one, while
// CPA's own in-request key swap keeps it and only replaces the API key (the
// rehearsal showed that swap would otherwise read as a re-login and cancel a
// due new_key_rejected disable).
func credentialFingerprint(doc map[string]any) string {
	sum := sha256.Sum256([]byte(dcaToken(doc)))
	return hex.EncodeToString(sum[:16])
}

func markDisabled(doc map[string]any, reason string, now time.Time) {
	doc["disabled"] = true
	doc["disabled_by"] = disabledByMarker
	doc["disabled_reason"] = reason
	doc["disabled_at"] = now.UTC().Format(time.RFC3339)
}

// mintedKey is the part of Meta's mint response CPA writes back.
type mintedKey struct {
	APIKey           string `json:"api_key"`
	BaseURL          string `json:"base_url"`
	UserEmail        string `json:"user_email"`
	UserFullName     string `json:"user_full_name"`
	SubsTierName     string `json:"subs_tier_name"`
	SubsTierID       string `json:"subs_tier_id"`
	IsSubsActive     bool   `json:"is_subs_active"`
	HasPaymentMethod bool   `json:"has_payment_method"`
}

func mintRequest(dca string) hostHTTPRequest {
	mintURL := strings.TrimSpace(os.Getenv("META_MINT_URL"))
	if mintURL == "" {
		mintURL = metaMintDefaultURL
	}
	body, _ := json.Marshal(map[string]string{"dca_token": dca})
	return hostHTTPRequest{
		Method: http.MethodPost,
		URL:    mintURL,
		Headers: map[string][]string{
			"Authorization": {"Bearer " + dca},
			"User-Agent":    {metaMintUserAgent},
			"Content-Type":  {"application/json"},
			"Accept":        {"application/json"},
		},
		Body: body,
	}
}

// applyMintedKey mirrors CPA's MetaExecutor.Refresh; every other field is kept.
func applyMintedKey(doc map[string]any, minted mintedKey, now time.Time) {
	apiKey := strings.TrimSpace(minted.APIKey)
	doc["api_key"] = apiKey
	doc["access_token"] = apiKey
	delete(doc, "expired")
	if base := safeMetaBaseURL(minted.BaseURL); base != "" {
		doc["base_url"] = base
	}
	if email := strings.TrimSpace(minted.UserEmail); email != "" {
		doc["email"] = email
	}
	if name := strings.TrimSpace(minted.UserFullName); name != "" {
		doc["name"] = name
	}
	for key, value := range map[string]string{"subs_tier_name": minted.SubsTierName, "subs_tier_id": minted.SubsTierID} {
		if value = strings.TrimSpace(value); value != "" {
			doc[key] = value
		} else {
			delete(doc, key)
		}
	}
	doc["is_subs_active"] = minted.IsSubsActive
	doc["has_payment_method"] = minted.HasPaymentMethod
	doc["type"] = "meta"
	doc["last_refresh"] = now.UTC().Format(time.RFC3339)
}

// safeMetaBaseURL accepts only https on meta.ai (same rule as supernewapi).
func safeMetaBaseURL(raw string) string {
	text := strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(text)
	if err != nil || parsed.Scheme != "https" {
		return ""
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "meta.ai" || strings.HasSuffix(host, ".meta.ai") {
		return text
	}
	return ""
}

func trimMessage(text string) string {
	text = strings.TrimSpace(text)
	if utf8.RuneCountInString(text) <= healthMessageLimit {
		return text
	}
	runes := []rune(text)
	return string(runes[:healthMessageLimit])
}

// ---- GET /plugins/usage-statistics/health -------------------------------------

func healthGet() ([]byte, error) {
	autoDisable := metaAutoDisable.Load()
	resp := HealthResponse{AutoDisable: autoDisable, Accounts: []AccountHealth{}, LastFailures: []AccountFailure{}}
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(200, resp))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	// With the switch off nothing is being checked any more: only the
	// credentials this plugin disabled still mean something.
	where := `ORDER BY auth_id`
	if !autoDisable {
		where = `WHERE state = 'disabled' ORDER BY auth_id`
	}
	rows, err := store.healthRowsWhere(ctx, where)
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query account health"}))
	}
	for _, r := range rows {
		resp.Accounts = append(resp.Accounts, AccountHealth{
			AuthID: r.AuthID, AuthIndex: r.AuthIndex, State: r.State, Reason: r.Reason, HTTPStatus: r.HTTPStatus,
			Message: r.Message, Since: r.First401At, UpdatedAt: r.UpdatedAt,
		})
	}
	failures, err := store.lastFailures(ctx, healthNow().Add(-lastFailureWindow))
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query account failures"}))
	}
	resp.LastFailures = failures
	return okEnvelope(jsonManagementResponse(200, resp))
}

func (s *SQLiteStore) lastFailures(ctx context.Context, since time.Time) ([]AccountFailure, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT auth_id, auth_index, status_code, message, at FROM account_last_failure
WHERE at >= ? ORDER BY auth_id`, formatTimestamp(since))
	if err != nil {
		return nil, fmt.Errorf("usage sqlite query last failures: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []AccountFailure{}
	for rows.Next() {
		var (
			f      AccountFailure
			atText string
		)
		if err := rows.Scan(&f.AuthID, &f.AuthIndex, &f.StatusCode, &f.Message, &atText); err != nil {
			return nil, fmt.Errorf("usage sqlite scan last failure: %w", err)
		}
		f.At = parseOptionalTimestamp(atText)
		out = append(out, f)
	}
	return out, rows.Err()
}
