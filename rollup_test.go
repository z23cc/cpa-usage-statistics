package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---- helpers ---------------------------------------------------------------

func utc(day, hour, minute, second int) time.Time {
	return time.Date(2026, 3, day, hour, minute, second, 0, time.UTC)
}

func tp(t time.Time) *time.Time { return &t }

// insertBulk writes records in one writer transaction. It bypasses Insert's
// normalisation, so callers give explicit total tokens; the triggers still fire.
func insertBulk(tb testing.TB, store *SQLiteStore, recs []Record) {
	tb.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		tb.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO usage_records (id, timestamp, provider, model, source, auth_id, auth_index, auth_type, failed,
		input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens, total_tokens)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tb.Fatal(err)
	}
	for _, r := range recs {
		k := r.Tokens
		if _, err := stmt.Exec(r.ID, formatTimestamp(r.Timestamp), r.Provider, r.Model, r.Source, r.AuthID, r.AuthIndex, r.AuthType,
			boolToInt(r.Failed), k.InputTokens, k.OutputTokens, k.ReasoningTokens, k.CachedTokens, k.CacheReadTokens,
			k.CacheCreationTokens, k.TotalTokens); err != nil {
			tb.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func queryStrings(tb testing.TB, db *sql.DB, query string, args ...any) []string {
	tb.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		tb.Fatalf("%s: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			tb.Fatal(err)
		}
		cells := make([]string, len(vals))
		for i, v := range vals {
			cells[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(cells, "|"))
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	return out
}

func rollupDump(t *testing.T, store *SQLiteStore, table string) []string {
	t.Helper()
	return queryStrings(t, store.db, "SELECT * FROM "+table+" ORDER BY bucket, auth_index, auth_id, model")
}

const rollupCountCols = "requests, failed, input_tokens, output_tokens, reasoning_tokens, cached_tokens, cache_read_tokens, cache_creation_tokens, total_tokens, pure_input_tokens"

// assertRollupsMatchDetails checks both rollups against a GROUP BY over the details.
func assertRollupsMatchDetails(t *testing.T, store *SQLiteStore) {
	t.Helper()
	for _, rt := range rollupTables {
		got := queryStrings(t, store.db, fmt.Sprintf(`SELECT bucket, auth_index, auth_id, model, %s FROM %s ORDER BY 1, 2, 3, 4`, rollupCountCols, rt.name))
		want := queryStrings(t, store.db, fmt.Sprintf(`SELECT substr(timestamp, 1, %d) b, auth_index, auth_id, model, COUNT(*), SUM(failed),
			SUM(input_tokens), SUM(output_tokens), SUM(reasoning_tokens), SUM(cached_tokens), SUM(cache_read_tokens), SUM(cache_creation_tokens),
			SUM(total_tokens), SUM(%s) FROM usage_records GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4`, rt.bucketLen, pureInputSQL))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s != details\n got %v\nwant %v", rt.name, got, want)
		}
	}
}

func scalarInt(t testing.TB, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// refAccountSummary is the pre-rollup AccountSummary (git show d67b267:store.go):
// one plain GROUP BY over usage_records.
func refAccountSummary(tb testing.TB, s *SQLiteStore, rng QueryRange) AccountsResponse {
	tb.Helper()
	result := AccountsResponse{Accounts: []AccountUsage{}}
	where, args := rangeWhere(rng)
	query := `
SELECT auth_index, auth_id, model, MAX(source), MAX(provider), MAX(auth_type),
       COUNT(*), SUM(failed),
       SUM(input_tokens), SUM(output_tokens), SUM(reasoning_tokens), SUM(cached_tokens),
       SUM(cache_read_tokens), SUM(cache_creation_tokens), SUM(total_tokens),
       SUM(` + pureInputSQL + `),
       MIN(timestamp), MAX(timestamp),
       SUM(COALESCE(cost_usd, 0)), SUM(CASE WHEN cost_usd IS NULL THEN 1 ELSE 0 END)
FROM usage_records`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY auth_index, auth_id, model"
	rows, err := s.db.Query(query, args...)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	type accountKey struct{ authIndex, authID string }
	accounts := make(map[accountKey]*AccountUsage)
	for rows.Next() {
		var group AccountUsage
		var model, firstText, lastText string
		var pureInput, unpriced int64
		var cost float64
		if err := rows.Scan(&group.AuthIndex, &group.AuthID, &model, &group.Source, &group.Provider, &group.AuthType,
			&group.Requests, &group.Failed,
			&group.Tokens.InputTokens, &group.Tokens.OutputTokens, &group.Tokens.ReasoningTokens, &group.Tokens.CachedTokens,
			&group.Tokens.CacheReadTokens, &group.Tokens.CacheCreationTokens, &group.Tokens.TotalTokens,
			&pureInput, &firstText, &lastText, &cost, &unpriced); err != nil {
			tb.Fatal(err)
		}
		group.FirstRequestAt, _ = parseStoredTimestamp(firstText)
		group.LastRequestAt, _ = parseStoredTimestamp(lastText)
		modelUsage := ModelUsage{Model: model, Requests: group.Requests, Success: group.Requests - group.Failed,
			Failed: group.Failed, Tokens: group.Tokens}
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
		// Costs are the ones fixed when each record was written.
		if group.Requests > unpriced {
			account.CostUSD += cost
			rounded := roundUSD(cost)
			modelUsage.CostUSD = &rounded
		}
		if unpriced > 0 {
			account.UnpricedModels = append(account.UnpricedModels, model)
		}
		account.Models = append(account.Models, modelUsage)
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
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
	return result
}

// randomRecords makes n records over days days from base. Some records land exactly on hour/day
// boundaries (or 1ns before), and some share a timestamp with the previous one.
func randomRecords(rng *rand.Rand, base time.Time, days, n int) []Record {
	accounts := []struct{ index, id, source, authType string }{
		{"0", "id-a", "src-a", "oauth"}, {"1", "id-b", "src-b", "apikey"}, {"2", "id-c", "src-c", "oauth"},
		{"2", "id-d", "src-d", "apikey"}, {"", "", "", ""},
	}
	models := []string{"priced-a", "priced-b", "unpriced-c", "claude-x"}
	providers := []string{"claude", "codex", "anthropic-api", "gemini"}
	span := int64(days) * 86400
	recs := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		var ts time.Time
		switch {
		case i > 0 && rng.Intn(10) == 0:
			ts = recs[i-1].Timestamp // tie
		default:
			sec := rng.Int63n(span)
			switch rng.Intn(10) {
			case 0:
				sec -= sec % 3600
			case 1:
				sec -= sec % 86400
			}
			ts = base.Add(time.Duration(sec) * time.Second)
			if rng.Intn(25) == 0 {
				ts = ts.Add(-time.Nanosecond)
				if ts.Before(base) {
					ts = base
				}
			}
		}
		acc := accounts[rng.Intn(len(accounts))]
		in := rng.Int63n(5000)
		cached := rng.Int63n(3000) // may exceed input: exercises the MAX(..., 0)
		create := rng.Int63n(500)
		out := rng.Int63n(2000)
		recs = append(recs, Record{
			ID: fmt.Sprintf("r%05d", i), Timestamp: ts,
			AuthIndex: acc.index, AuthID: acc.id, Source: acc.source, AuthType: acc.authType,
			Model: models[rng.Intn(len(models))], Provider: providers[rng.Intn(len(providers))],
			Failed: rng.Intn(7) == 0,
			Tokens: TokenStats{InputTokens: in, OutputTokens: out, ReasoningTokens: rng.Int63n(300), CachedTokens: cached,
				CacheReadTokens: rng.Int63n(100), CacheCreationTokens: create, TotalTokens: in + out + 1},
		})
	}
	return recs
}

// ---- 1. equivalence --------------------------------------------------------

func TestAccountSummaryEquivalentToGroupBy(t *testing.T) {
	store := newTestStore(t)
	rng := rand.New(rand.NewSource(20260310))
	base := utc(10, 0, 0, 0)
	// Prices first: costs are fixed when a record is written.
	putPrices(t, store,
		ModelPrice{Model: "priced-a", Input: 2, Output: 8, CacheRead: 0.5, CacheCreation: 1},
		ModelPrice{Model: "priced-b", Input: 3.5, Output: 7.25, CacheRead: 0.1, CacheCreation: 0.7},
		ModelPrice{Model: "claude-x", Input: 15, Output: 75, CacheRead: 1.5, CacheCreation: 18.75})
	insertBulk(t, store, randomRecords(rng, base, 4, 3000))
	assertRollupsMatchDetails(t, store)

	randMinute := func() time.Time { // -6h .. 4d+6h, minute precision
		m := rng.Intn(4*1440+720) - 360
		ts := base.Add(time.Duration(m) * time.Minute)
		switch rng.Intn(6) {
		case 0, 1:
			ts = ts.Truncate(time.Hour)
		case 2:
			ts = floorDay(ts)
		}
		return ts
	}
	ctx := context.Background()
	check := func(rngQ QueryRange) {
		t.Helper()
		got, err := store.AccountSummary(ctx, rngQ)
		if err != nil {
			t.Fatal(err)
		}
		got.DetailsSince = nil
		want := refAccountSummary(t, store, rngQ)
		if !reflect.DeepEqual(got, want) {
			gj, _ := json.Marshal(got)
			wj, _ := json.Marshal(want)
			t.Fatalf("range %s..%s\n got %s\nwant %s", fmtPtr(rngQ.Start), fmtPtr(rngQ.End), gj, wj)
		}
	}
	check(QueryRange{})
	nonEmpty := 0
	for i := 0; i < 300; i++ {
		var q QueryRange
		if rng.Intn(8) != 0 {
			q.Start = tp(randMinute())
		}
		if rng.Intn(8) != 0 {
			q.End = tp(randMinute())
		}
		if q.Start != nil && q.End != nil && rng.Intn(10) != 0 && q.End.Before(*q.Start) {
			q.Start, q.End = q.End, q.Start // mostly ordered, sometimes start > end
		}
		check(q)
		if q.Start != nil && q.End != nil && q.Start.Before(*q.End) {
			nonEmpty++
		}
	}
	if nonEmpty < 150 {
		t.Fatalf("only %d ordered ranges", nonEmpty)
	}
}

func fmtPtr(p *time.Time) string {
	if p == nil {
		return "nil"
	}
	return p.Format(time.RFC3339)
}

// A range wholly before the first record, inside the first record's hour, must
// be empty: with no retention the cleaned part of the hour is empty too.
func TestAccountSummaryRangeBeforeFirstRecordSameHour(t *testing.T) {
	store := newTestStore(t)
	insertAll(t, store, Record{ID: "first", Timestamp: utc(10, 10, 37, 0), AuthIndex: "x", Tokens: tok(1, 1, 0, 0, 0, 0, 2)})
	q := QueryRange{Start: tp(utc(10, 10, 10, 0)), End: tp(utc(10, 10, 20, 0))}
	got, err := store.AccountSummary(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if want := refAccountSummary(t, store, q); !reflect.DeepEqual(got.Accounts, want.Accounts) {
		t.Fatalf("got %+v\nwant %+v", got.Accounts, want.Accounts)
	}
}

// ---- 2. planner ------------------------------------------------------------

func TestPlanSummaryPieces(t *testing.T) {
	raw := func(a, b time.Time) summaryPiece {
		return summaryPiece{rawSummaryKey, formatTimestamp(a), formatTimestamp(b)}
	}
	hr := func(a, b time.Time) summaryPiece { return summaryPiece{"usage_hourly", hourKey(a), hourKey(b)} }
	cl := func(h time.Time) summaryPiece { return summaryPiece{cleanedHourKey, hourKey(h), ""} }
	dy := func(a, b time.Time) summaryPiece { return summaryPiece{"usage_daily", dayKey(a), dayKey(b)} }
	early := tp(utc(1, 0, 0, 0)) // details reach back before every range below
	cases := []struct {
		name       string
		start, end *time.Time
		since      *time.Time
		want       []summaryPiece
	}{
		{"same hour", tp(utc(10, 10, 10, 0)), tp(utc(10, 10, 50, 0)), early,
			[]summaryPiece{raw(utc(10, 10, 10, 0), utc(10, 10, 50, 0))}},
		{"crossing one hour", tp(utc(10, 10, 30, 0)), tp(utc(10, 11, 20, 0)), early,
			[]summaryPiece{raw(utc(10, 10, 30, 0), utc(10, 11, 0, 0)), raw(utc(10, 11, 0, 0), utc(10, 11, 20, 0))}},
		{"aligned hours", tp(utc(10, 10, 0, 0)), tp(utc(10, 12, 0, 0)), early,
			[]summaryPiece{hr(utc(10, 10, 0, 0), utc(10, 12, 0, 0))}},
		{"aligned single hour", tp(utc(10, 10, 0, 0)), tp(utc(10, 11, 0, 0)), early,
			[]summaryPiece{hr(utc(10, 10, 0, 0), utc(10, 11, 0, 0))}},
		{"midnight without a whole day", tp(utc(10, 22, 30, 0)), tp(utc(11, 1, 30, 0)), early,
			[]summaryPiece{raw(utc(10, 22, 30, 0), utc(10, 23, 0, 0)), raw(utc(11, 1, 0, 0), utc(11, 1, 30, 0)),
				hr(utc(10, 23, 0, 0), utc(11, 1, 0, 0))}},
		{"aligned whole day", tp(utc(10, 0, 0, 0)), tp(utc(11, 0, 0, 0)), early,
			[]summaryPiece{dy(utc(10, 0, 0, 0), utc(11, 0, 0, 0))}},
		{"multi day", tp(utc(10, 22, 30, 0)), tp(utc(13, 1, 30, 0)), early,
			[]summaryPiece{raw(utc(10, 22, 30, 0), utc(10, 23, 0, 0)), raw(utc(13, 1, 0, 0), utc(13, 1, 30, 0)),
				hr(utc(10, 23, 0, 0), utc(11, 0, 0, 0)), dy(utc(11, 0, 0, 0), utc(13, 0, 0, 0)), hr(utc(13, 0, 0, 0), utc(13, 1, 0, 0))}},
		{"open end", tp(utc(10, 10, 30, 0)), nil, early,
			[]summaryPiece{raw(utc(10, 10, 30, 0), utc(10, 11, 0, 0)), hr(utc(10, 11, 0, 0), utc(11, 0, 0, 0)),
				{"usage_daily", dayKey(utc(11, 0, 0, 0)), ""}}},
		{"open end aligned day", tp(utc(11, 0, 0, 0)), nil, early,
			[]summaryPiece{{"usage_daily", dayKey(utc(11, 0, 0, 0)), ""}}},
		{"open start", nil, tp(utc(13, 1, 30, 0)), early,
			[]summaryPiece{raw(utc(13, 1, 0, 0), utc(13, 1, 30, 0)), {"usage_daily", "", dayKey(utc(13, 0, 0, 0))},
				hr(utc(13, 0, 0, 0), utc(13, 1, 0, 0))}},
		{"open both", nil, nil, early, []summaryPiece{{"usage_daily", "", ""}}},
		{"open both no details", nil, nil, nil, []summaryPiece{{"usage_daily", "", ""}}},
		{"start == end", tp(utc(10, 10, 10, 0)), tp(utc(10, 10, 10, 0)), early, nil},
		{"start > end", tp(utc(10, 12, 0, 0)), tp(utc(10, 10, 0, 0)), early, nil},
		{"detailsSince nil adds cleaned hour, same hour", tp(utc(10, 10, 10, 0)), tp(utc(10, 10, 50, 0)), nil,
			[]summaryPiece{raw(utc(10, 10, 10, 0), utc(10, 10, 50, 0)), cl(utc(10, 10, 0, 0))}},
		{"detailsSince nil adds cleaned hour at both ends", tp(utc(10, 10, 25, 0)), tp(utc(10, 13, 40, 0)), nil,
			[]summaryPiece{raw(utc(10, 10, 25, 0), utc(10, 11, 0, 0)), cl(utc(10, 10, 0, 0)),
				raw(utc(10, 13, 0, 0), utc(10, 13, 40, 0)), cl(utc(10, 13, 0, 0)), hr(utc(10, 11, 0, 0), utc(10, 13, 0, 0))}},
		{"straddle start", tp(utc(10, 10, 25, 0)), tp(utc(10, 12, 0, 0)), tp(utc(10, 10, 37, 0)),
			[]summaryPiece{raw(utc(10, 10, 25, 0), utc(10, 11, 0, 0)), cl(utc(10, 10, 0, 0)), hr(utc(10, 11, 0, 0), utc(10, 12, 0, 0))}},
		{"same hour before detailsSince", tp(utc(10, 10, 10, 0)), tp(utc(10, 10, 20, 0)), tp(utc(10, 10, 37, 0)),
			[]summaryPiece{raw(utc(10, 10, 10, 0), utc(10, 10, 20, 0)), cl(utc(10, 10, 0, 0))}},
		{"start exactly at detailsSince stays in details", tp(utc(10, 10, 37, 0)), tp(utc(10, 12, 0, 0)), tp(utc(10, 10, 37, 0)),
			[]summaryPiece{raw(utc(10, 10, 37, 0), utc(10, 11, 0, 0)), hr(utc(10, 11, 0, 0), utc(10, 12, 0, 0))}},
		{"whole range older than detailsSince", tp(utc(10, 10, 25, 0)), tp(utc(10, 13, 40, 0)), tp(utc(12, 0, 0, 0)),
			[]summaryPiece{raw(utc(10, 10, 25, 0), utc(10, 11, 0, 0)), cl(utc(10, 10, 0, 0)),
				raw(utc(10, 13, 0, 0), utc(10, 13, 40, 0)), cl(utc(10, 13, 0, 0)), hr(utc(10, 11, 0, 0), utc(10, 13, 0, 0))}},
		{"end edge at detailsSince adds no cleaned hour", tp(utc(10, 10, 0, 0)), tp(utc(10, 12, 30, 0)), tp(utc(10, 12, 0, 0)),
			[]summaryPiece{raw(utc(10, 12, 0, 0), utc(10, 12, 30, 0)), hr(utc(10, 10, 0, 0), utc(10, 12, 0, 0))}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := planSummaryPieces(c.start, c.end, c.since)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got  %+v\nwant %+v", got, c.want)
			}
		})
	}
	t.Run("non-UTC input", func(t *testing.T) {
		zone := time.FixedZone("x", 8*3600)
		got := planSummaryPieces(tp(utc(10, 10, 0, 0).In(zone)), tp(utc(10, 12, 0, 0).In(zone)), early)
		want := []summaryPiece{hr(utc(10, 10, 0, 0), utc(10, 12, 0, 0))}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v want %+v", got, want)
		}
	})
}

// ---- 3. retention ----------------------------------------------------------

func retentionFixture(t *testing.T, store *SQLiteStore) (oldCount int, cutoff time.Time) {
	t.Helper()
	const n = cleanupBatchSize + 51 // more than one batch
	rng := rand.New(rand.NewSource(7))
	old := randomRecords(rng, utc(1, 0, 0, 0), 9, n) // 03-01 .. 03-10 00:00
	old = append(old, Record{ID: "old-1205", Timestamp: utc(10, 12, 5, 0), AuthIndex: "0", AuthID: "id-a", Model: "gone-model",
		Provider: "codex", Tokens: tok(100, 10, 0, 0, 0, 0, 110)})
	fresh := []Record{
		{ID: "n1", Timestamp: utc(10, 12, 37, 0), AuthIndex: "0", AuthID: "id-a", Model: "kept", Provider: "codex", Tokens: tok(10, 1, 0, 0, 0, 0, 11)},
		{ID: "n2", Timestamp: utc(10, 12, 50, 0), AuthIndex: "1", AuthID: "id-b", Model: "kept", Provider: "claude", Tokens: tok(20, 2, 0, 0, 0, 0, 22)},
		{ID: "n3", Timestamp: utc(10, 15, 10, 0), AuthIndex: "0", AuthID: "id-a", Model: "kept", Provider: "codex", Tokens: tok(30, 3, 0, 0, 0, 0, 33), Failed: true},
		{ID: "n4", Timestamp: utc(11, 2, 0, 0), AuthIndex: "2", AuthID: "id-c", Model: "kept", Provider: "codex", Tokens: tok(40, 4, 0, 0, 0, 0, 44)},
	}
	insertBulk(t, store, append(old, fresh...))
	return len(old), utc(10, 12, 20, 0)
}

func TestDeleteBeforeKeepsRollups(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	oldCount, cutoff := retentionFixture(t, store)
	assertRollupsMatchDetails(t, store)
	hourlyBefore, dailyBefore := rollupDump(t, store, "usage_hourly"), rollupDump(t, store, "usage_daily")
	summaryBefore, err := store.AccountSummary(ctx, QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	if summaryBefore.DetailsSince == nil || summaryBefore.DetailsSince.Before(utc(1, 0, 0, 0)) || summaryBefore.DetailsSince.After(utc(1, 6, 0, 0)) {
		t.Fatalf("details_since before = %v", summaryBefore.DetailsSince)
	}

	deleted, err := store.DeleteBefore(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != int64(oldCount) {
		t.Fatalf("deleted = %d want %d (more than one batch of %d)", deleted, oldCount, cleanupBatchSize)
	}
	if left := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`); left != 4 {
		t.Fatalf("details left = %d", left)
	}
	if !reflect.DeepEqual(hourlyBefore, rollupDump(t, store, "usage_hourly")) || !reflect.DeepEqual(dailyBefore, rollupDump(t, store, "usage_daily")) {
		t.Fatal("rollups changed by retention")
	}
	if keep := scalarInt(t, store.db, `SELECT keep_on_delete FROM usage_rollup_control WHERE id = 1`); keep != 0 {
		t.Fatalf("keep_on_delete = %d", keep)
	}
	if again, err := store.DeleteBefore(ctx, cutoff); err != nil || again != 0 {
		t.Fatalf("second DeleteBefore = %d, %v", again, err)
	}

	after, err := store.AccountSummary(ctx, QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	if after.DetailsSince == nil || !after.DetailsSince.Equal(utc(10, 12, 37, 0)) {
		t.Fatalf("details_since after = %v", after.DetailsSince)
	}
	if after.Totals != summaryBefore.Totals || !reflect.DeepEqual(after.Accounts, summaryBefore.Accounts) {
		t.Fatalf("totals changed by retention:\n got %+v\nwant %+v", after.Totals, summaryBefore.Totals)
	}

	// Starts before details_since (12:37): the retained 12:37 and 12:50 come from
	// the details, and the cleaned part of hour 12 (the deleted 12:05 record, the
	// hour rollup minus kept details) is added whole. The count is still 5 as
	// under the old whole-hour snap, but now reached as 2 exact + 1 cleaned + 2 later.
	wantRequests := scalarInt(t, store.db, `SELECT SUM(requests) FROM usage_hourly WHERE bucket >= '2026-03-10T12'`)
	snapped, err := store.AccountSummary(ctx, QueryRange{Start: tp(utc(10, 12, 25, 0))})
	if err != nil {
		t.Fatal(err)
	}
	if snapped.Totals.Requests != wantRequests || wantRequests != 5 {
		t.Fatalf("snapped requests = %d, hourly = %d (want 5: 4 retained + deleted 12:05)", snapped.Totals.Requests, wantRequests)
	}
	// At or after details_since the details answer exactly.
	exact, err := store.AccountSummary(ctx, QueryRange{Start: tp(utc(10, 12, 45, 0))})
	if err != nil {
		t.Fatal(err)
	}
	if exact.Totals.Requests != 3 {
		t.Fatalf("exact requests = %d, want 3", exact.Totals.Requests)
	}
}

// ---- 4. manual delete ------------------------------------------------------

func TestDeleteDecrementsRollups(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	insertAll(t, store,
		Record{ID: "d1", Timestamp: utc(10, 10, 5, 0), AuthIndex: "a", AuthID: "A", Model: "m1", Provider: "codex", Failed: true, Tokens: tok(100, 10, 1, 30, 5, 7, 118)},
		Record{ID: "d2", Timestamp: utc(10, 10, 6, 0), AuthIndex: "a", AuthID: "A", Model: "m1", Provider: "claude", Tokens: tok(50, 5, 0, 10, 0, 2, 55)},
		Record{ID: "d3", Timestamp: utc(10, 10, 7, 0), AuthIndex: "a", AuthID: "A", Model: "m2", Provider: "codex", Tokens: tok(9, 1, 0, 0, 0, 0, 10)},
		Record{ID: "d4", Timestamp: utc(10, 15, 0, 0), AuthIndex: "a", AuthID: "A", Model: "m1", Provider: "codex", Tokens: tok(1, 1, 0, 0, 0, 0, 2)},
	)
	assertRollupsMatchDetails(t, store)
	res, err := store.Delete(ctx, []string{"d1", "d3", "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 2 || len(res.Missing) != 1 {
		t.Fatalf("delete = %+v", res)
	}
	assertRollupsMatchDetails(t, store)
	// d3 emptied its bucket, so the row is gone; d1 left d2's totals behind.
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_hourly WHERE model = 'm2'`); n != 0 {
		t.Fatalf("emptied hourly row kept: %d", n)
	}
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_daily WHERE model = 'm2'`); n != 0 {
		t.Fatalf("emptied daily row kept: %d", n)
	}
	got := queryStrings(t, store.db, `SELECT bucket, `+rollupCountCols+` FROM usage_hourly WHERE bucket = '2026-03-10T10'`)
	// d2 only: claude provider counts all 50 input as pure input.
	if want := []string{"2026-03-10T10|1|0|50|5|0|10|0|2|55|50"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("hourly = %v want %v", got, want)
	}
	if _, err := store.Delete(ctx, []string{"d2", "d4"}); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, store.db, `SELECT (SELECT COUNT(*) FROM usage_hourly) + (SELECT COUNT(*) FROM usage_daily)`); n != 0 {
		t.Fatalf("rollup rows left: %d", n)
	}
}

// ---- 5. backfill and migration --------------------------------------------

func TestBackfillMigration(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	rng := rand.New(rand.NewSource(99))
	recs := randomRecords(rng, utc(10, 0, 0, 0), 3, 1500)

	// reference: the same records through the triggers.
	ref := newTestStore(t)
	insertBulk(t, ref, recs)

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	insertBulk(t, store, recs)
	for _, stmt := range []string{
		`DROP TRIGGER usage_records_rollup_ai`, `DROP TRIGGER usage_records_rollup_ad`,
		`DROP TABLE usage_hourly`, `DROP TABLE usage_daily`, `DROP TABLE usage_rollup_control`,
		`PRAGMA user_version = 0`,
	} {
		if _, err := store.db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if v := scalarInt(t, store.db, `PRAGMA user_version`); v != rollupSchemaVersion {
		t.Fatalf("user_version = %d", v)
	}
	assertRollupsMatchDetails(t, store)
	for _, table := range []string{"usage_hourly", "usage_daily"} {
		if got, want := rollupDump(t, store, table), rollupDump(t, ref, table); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: backfill differs from trigger-built rollup", table)
		}
	}
	hourly, daily := rollupDump(t, store, "usage_hourly"), rollupDump(t, store, "usage_daily")

	// Reopen: version 1 is not backfilled again.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err = NewSQLiteStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if !reflect.DeepEqual(hourly, rollupDump(t, store, "usage_hourly")) || !reflect.DeepEqual(daily, rollupDump(t, store, "usage_daily")) {
		t.Fatal("reopen changed the rollups")
	}
	assertRollupsMatchDetails(t, store)

	// An older plugin writes with plain SQL: the triggers (in the db) keep up.
	if _, err := store.db.Exec(`INSERT INTO usage_records (id, timestamp, provider, model, auth_index, auth_id, input_tokens, total_tokens)
		VALUES ('legacy', ?, 'codex', 'm-legacy', 'z', 'Z', 40, 40)`, formatTimestamp(utc(11, 5, 6, 7))); err != nil {
		t.Fatal(err)
	}
	assertRollupsMatchDetails(t, store)
	if n := scalarInt(t, store.db, `SELECT requests FROM usage_daily WHERE model = 'm-legacy'`); n != 1 {
		t.Fatalf("legacy insert not rolled up: %d", n)
	}
	if _, err := store.db.Exec(`DELETE FROM usage_records WHERE id = 'legacy'`); err != nil {
		t.Fatal(err)
	}
	assertRollupsMatchDetails(t, store)
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_daily WHERE model = 'm-legacy'`); n != 0 {
		t.Fatalf("legacy delete not rolled back: %d", n)
	}
}

// ---- 6. WAL and connections ------------------------------------------------

func TestWALAndReaderPool(t *testing.T) {
	store := newTestStore(t)
	var mode string
	if err := store.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q, %v", mode, err)
	}
	insertAll(t, store, Record{ID: "base", Timestamp: utc(10, 1, 0, 0), AuthIndex: "a"})

	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO usage_records (id, timestamp, auth_index) VALUES ('held', ?, 'a')`, formatTimestamp(utc(10, 2, 0, 0))); err != nil {
		t.Fatal(err)
	}
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var n int64
		err := store.rdb.QueryRowContext(ctx, `SELECT COUNT(*) FROM usage_records`).Scan(&n)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Fatalf("read during write = %d, %v (want the committed snapshot, 1)", r.n, r.err)
		}
	case <-time.After(10 * time.Second): // timeout guard only
		t.Fatal("read blocked behind the open write tx")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, store.rdb, `SELECT COUNT(*) FROM usage_records`); n != 2 {
		t.Fatalf("after commit = %d", n)
	}

	if _, err := store.rdb.Exec(`INSERT INTO usage_records (id, timestamp) VALUES ('x', 'y')`); err == nil {
		t.Fatal("write through the read pool succeeded")
	}
	if _, err := store.rdb.Exec(`DELETE FROM usage_records`); err == nil {
		t.Fatal("delete through the read pool succeeded")
	}
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`); n != 2 {
		t.Fatalf("rows = %d", n)
	}
}

func assertBlocked(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("Close returned while %s", what)
	case <-time.After(150 * time.Millisecond): // negative wait: Close must not finish
	}
}

func assertFinishes(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second): // timeout guard only
		t.Fatalf("Close did not return after %s", what)
	}
}

func TestCloseWaitsForInFlightRead(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	insertAll(t, store,
		Record{ID: "a", Timestamp: utc(10, 1, 0, 0)}, Record{ID: "b", Timestamp: utc(10, 2, 0, 0)})
	rows, err := store.rdb.Query(`SELECT id FROM usage_records ORDER BY timestamp`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("no rows")
	}
	done := make(chan struct{})
	go func() { _ = store.Close(); close(done) }()
	assertBlocked(t, done, "a read iterator was still open")
	_ = rows.Close()
	assertFinishes(t, done, "the read finished")
}

func TestCloseWaitsForInFlightWrite(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO usage_records (id, timestamp) VALUES ('w', ?)`, formatTimestamp(utc(10, 1, 0, 0))); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = store.Close(); close(done) }()
	assertBlocked(t, done, "a write tx was still open")
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertFinishes(t, done, "the write committed")
}

func TestCloseCheckpointsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	insertAll(t, store, Record{ID: "a", Timestamp: utc(10, 1, 0, 0), AuthIndex: "a"})
	if _, err := store.AccountSummary(context.Background(), QueryRange{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Errorf("usage.db%s after clean Close: %v", suffix, err)
		}
	}
	// The data survived the checkpoint.
	again, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = again.Close() }()
	if n := scalarInt(t, again.db, `SELECT COUNT(*) FROM usage_records`); n != 1 {
		t.Fatalf("rows after reopen = %d", n)
	}
}

// ---- 7. cleanup goroutine --------------------------------------------------

func pollUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func cleanupChan() chan struct{} {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	return cleanupDone
}

func TestStartCleanupDeletesOldRows(t *testing.T) {
	call := lifecycle(t)
	dir := t.TempDir()
	seed, err := NewSQLiteStore(filepath.Join(dir, "usage.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	insertAll(t, seed,
		Record{ID: "old", Timestamp: now.Add(-10 * 24 * time.Hour), AuthIndex: "a", Model: "gone"},
		Record{ID: "new", Timestamp: now.Add(-time.Hour), AuthIndex: "a", Model: "kept"})
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	call("plugin.register", map[string]any{"config_yaml": "data_dir: '" + dir + "'\nretention_days: 1\n"})
	store := currentStore()
	pollUntil(t, "retention cleanup", func() bool {
		return scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`) == 1
	})
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records WHERE id = 'new'`); n != 1 {
		t.Fatal("recent row was deleted")
	}
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_daily WHERE model = 'gone'`); n != 1 {
		t.Fatal("rollup lost the cleaned-up row")
	}

	done := cleanupChan()
	if done == nil {
		t.Fatal("cleanup goroutine not started")
	}
	finished := make(chan struct{})
	go func() { stopCleanup(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second): // timeout guard only
		t.Fatal("stopCleanup did not return")
	}
	select {
	case <-done:
	default:
		t.Fatal("stopCleanup returned before the goroutine exited")
	}
	if cleanupChan() != nil {
		t.Fatal("cleanup state not cleared")
	}
	stopCleanup() // idempotent

	// closeStore stops it, and a store without retention starts none.
	call("plugin.reconfigure", map[string]any{"config_yaml": "data_dir: '" + dir + "'\nretention_days: 1\n"})
	done = cleanupChan()
	if done == nil {
		t.Fatal("reconfigure did not restart the cleanup")
	}
	closeStore()
	select {
	case <-done:
	default:
		t.Fatal("closeStore did not stop the cleanup")
	}
	call("plugin.reconfigure", map[string]any{"config_yaml": "data_dir: '" + dir + "'\nretention_days: 0\n"})
	if cleanupChan() != nil {
		t.Fatal("cleanup running with retention disabled")
	}
}

func TestHandleUsageDoesNotCleanUp(t *testing.T) {
	closeStore()
	store := newTestStore(t)
	useStore(t, store)
	retentionDays.Store(1)
	t.Cleanup(func() { retentionDays.Store(0) })
	insertAll(t, store, Record{ID: "old", Timestamp: time.Now().Add(-30 * 24 * time.Hour), AuthIndex: "a"})
	body, _ := json.Marshal(map[string]any{"Provider": "codex", "Model": "m", "AuthIndex": "a", "RequestedAt": time.Now()})
	if _, err := handleUsage(body); err != nil {
		t.Fatal(err)
	}
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`); n != 2 {
		t.Fatalf("rows = %d: handleUsage cleaned up", n)
	}
	if cleanupChan() != nil {
		t.Fatal("handleUsage started a cleanup")
	}
}

// ---- 8. prices -------------------------------------------------------------

func TestModelsSeenSurvivesRetention(t *testing.T) {
	store := newTestStore(t)
	insertAll(t, store,
		Record{ID: "o", Timestamp: utc(1, 1, 0, 0), AuthIndex: "a", Model: "gone-model"},
		Record{ID: "n", Timestamp: utc(10, 1, 0, 0), AuthIndex: "a", Model: "kept-model"})
	if n, err := store.DeleteBefore(context.Background(), utc(5, 0, 0, 0)); err != nil || n != 1 {
		t.Fatalf("DeleteBefore = %d, %v", n, err)
	}
	prices, err := store.Prices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"gone-model", "kept-model"}; !reflect.DeepEqual(prices.ModelsSeen, want) {
		t.Fatalf("models_seen = %v want %v", prices.ModelsSeen, want)
	}
}

// ---- 9. query plans --------------------------------------------------------

func TestSummaryPieceQueryPlans(t *testing.T) {
	store := newTestStore(t)
	rng := rand.New(rand.NewSource(5))
	insertBulk(t, store, randomRecords(rng, utc(1, 0, 0, 0), 20, 3000))
	plan := func(p summaryPiece) string {
		query, args := summaryPieceSQL(p)
		return explainPlan(t, store, query, argsAny(args)...)
	}
	for _, p := range []summaryPiece{
		{"usage_hourly", "2026-03-05T10", "2026-03-05T20"},
		{"usage_hourly", "2026-03-05T10", ""},
		{"usage_daily", "2026-03-05", "2026-03-15"},
		{"usage_daily", "", "2026-03-15"},
		{"usage_daily", "2026-03-05", ""},
	} {
		got := plan(p)
		if strings.Contains(got, "SCAN "+p.table) || !strings.Contains(got, "SEARCH "+p.table+" USING PRIMARY KEY") {
			t.Errorf("%s %q..%q plan:\n%s", p.table, p.from, p.to, got)
		}
	}
	got := plan(summaryPiece{rawSummaryKey, formatTimestamp(utc(5, 10, 30, 0)), formatTimestamp(utc(5, 11, 0, 0))})
	if !strings.Contains(got, "SEARCH usage_records USING INDEX idx_usage_records_timestamp") || strings.Contains(got, "SCAN usage_records") {
		t.Errorf("details edge plan:\n%s", got)
	}
}

func argsAny(a []any) []any { return a }

// ---- 10. benchmark ---------------------------------------------------------

// BenchmarkAccountSummary30Days: 30 days, 40 accounts x 8 models, 90k records,
// a minute-precision (non-aligned) 30-day range. "groupby" is the pre-rollup
// query over the same data for comparison.
func BenchmarkAccountSummary30Days(b *testing.B) {
	store, err := NewSQLiteStore(filepath.Join(b.TempDir(), "usage.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rng := rand.New(rand.NewSource(1))
	base := utc(1, 0, 0, 0)
	const total = 90000
	recs := make([]Record, total)
	for i := range recs {
		acc := rng.Intn(40)
		in := rng.Int63n(8000)
		recs[i] = Record{
			ID: fmt.Sprintf("b%06d", i), Timestamp: base.Add(time.Duration(rng.Int63n(30*86400)) * time.Second),
			AuthIndex: fmt.Sprint(acc), AuthID: fmt.Sprintf("id-%02d", acc), Source: "s", AuthType: "oauth",
			Model: fmt.Sprintf("model-%d", rng.Intn(8)), Provider: []string{"claude", "codex", "gemini"}[rng.Intn(3)],
			Tokens: TokenStats{InputTokens: in, OutputTokens: rng.Int63n(2000), CachedTokens: rng.Int63n(4000), TotalTokens: in + 1},
		}
	}
	insertBulk(b, store, recs)
	q := QueryRange{Start: tp(base.Add(2*time.Hour + 17*time.Minute)), End: tp(base.Add(29*24*time.Hour + 5*time.Hour + 41*time.Minute))}
	got, err := store.AccountSummary(context.Background(), q)
	if err != nil {
		b.Fatal(err)
	}
	got.DetailsSince = nil
	if want := refAccountSummary(b, store, q); !reflect.DeepEqual(got, want) {
		b.Fatal("rollup summary differs from the group-by reference")
	}
	b.Run("rollup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := store.AccountSummary(context.Background(), q); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("groupby", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			refAccountSummary(b, store, q)
		}
	})
}

// ---- retention semantics of the cleaned hour -------------------------------

// cleanedFixture: hour 10 (day 10) keeps A@10:37 and A@10:50 and C@10:40;
// retention (cutoff 10:31) deleted A@10:05, A@10:30 and B@10:20, plus A@08:15
// and B@09:50 in earlier hours. Total tokens are distinct powers of two.
func cleanedFixture(t *testing.T) *SQLiteStore {
	t.Helper()
	store := newTestStore(t)
	rec := func(id string, h, m int, idx string, total int64) Record {
		return Record{ID: id, Timestamp: utc(10, h, m, 0), AuthIndex: idx, AuthID: "id-" + idx, Model: "m", Provider: "codex",
			Tokens: tok(total, 0, 0, 0, 0, 0, total)}
	}
	insertAll(t, store,
		rec("a0815", 8, 15, "A", 16), rec("b0950", 9, 50, "B", 64),
		rec("a1005", 10, 5, "A", 1), rec("b1020", 10, 20, "B", 32), rec("a1030", 10, 30, "A", 2),
		rec("a1037", 10, 37, "A", 4), rec("c1040", 10, 40, "C", 128), rec("a1050", 10, 50, "A", 8),
		rec("c1230", 12, 30, "C", 256))
	if n, err := store.DeleteBefore(context.Background(), utc(10, 10, 31, 0)); err != nil || n != 5 {
		t.Fatalf("DeleteBefore = %d, %v", n, err)
	}
	return store
}

// wantAccounts checks requests and total tokens per auth_index, and that no
// other account appears.
func wantAccounts(t *testing.T, store *SQLiteStore, q QueryRange, want map[string][2]int64) {
	t.Helper()
	got, err := store.AccountSummary(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string][2]int64{}
	for _, a := range got.Accounts {
		have[a.AuthIndex] = [2]int64{a.Requests, a.Tokens.TotalTokens}
	}
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("range %s..%s\n got %v\nwant %v", fmtPtr(q.Start), fmtPtr(q.End), have, want)
	}
}

func TestCleanedHourStraddle(t *testing.T) {
	store := cleanedFixture(t)
	// A: kept 10:37 + 10:50 exactly, plus cleaned 10:05 + 10:30 = 4 requests.
	// B was fully cleaned in this hour (no kept details): its 10:20 comes in
	// whole. C has only kept details, so subtracting them leaves nothing extra.
	wantAccounts(t, store, QueryRange{Start: tp(utc(10, 10, 25, 0)), End: tp(utc(10, 12, 0, 0))},
		map[string][2]int64{"A": {4, 15}, "B": {1, 32}, "C": {1, 128}})
}

func TestCleanedHourSameHourBeforeKept(t *testing.T) {
	store := cleanedFixture(t)
	// Only the cleaned records come back; kept 10:37/10:50/10:40 lie outside.
	wantAccounts(t, store, QueryRange{Start: tp(utc(10, 10, 10, 0)), End: tp(utc(10, 10, 20, 0))},
		map[string][2]int64{"A": {2, 3}, "B": {1, 32}})
}

func TestCleanedHoursWholeRangeOlderThanKept(t *testing.T) {
	store := cleanedFixture(t)
	// Nothing is kept before 10:37; both cleaned hours are included whole,
	// although A@08:15 precedes the 08:30 start.
	wantAccounts(t, store, QueryRange{Start: tp(utc(10, 8, 30, 0)), End: tp(utc(10, 9, 55, 0))},
		map[string][2]int64{"A": {1, 16}, "B": {1, 64}})
}

func TestCleanedHourEndEdge(t *testing.T) {
	store := cleanedFixture(t)
	// The end edge 10:00..10:35 lies before details_since: kept 10:37+ stay out,
	// the cleaned 10:05/10:20/10:30 are in, and hours 8-9 are whole.
	wantAccounts(t, store, QueryRange{End: tp(utc(10, 10, 35, 0))},
		map[string][2]int64{"A": {3, 19}, "B": {2, 96}})
}

// ---- review follow-ups: rollup init safety, control row, zero bounds --------

// rawExec runs statements on a plain connection to path (no plugin code).
func rawExec(t *testing.T, path string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func rawStrings(t *testing.T, path, query string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	return queryStrings(t, db, query)
}

const dumpHourly = `SELECT * FROM usage_hourly ORDER BY bucket, auth_index, auth_id, model`
const dumpDaily = `SELECT * FROM usage_daily ORDER BY bucket, auth_index, auth_id, model`

// retainedStore has 5 days of records with the first 3 days cleaned up, so the
// rollups hold history the details no longer have. It is closed on return.
func retainedStore(t *testing.T, path string) (hourly, daily []string) {
	t.Helper()
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	insertBulk(t, store, randomRecords(rand.New(rand.NewSource(11)), utc(1, 0, 0, 0), 5, 800))
	if n, err := store.DeleteBefore(context.Background(), utc(3, 0, 0, 0)); err != nil || n == 0 {
		t.Fatalf("DeleteBefore = %d, %v", n, err)
	}
	hourly, daily = rollupDump(t, store, "usage_hourly"), rollupDump(t, store, "usage_daily")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return hourly, daily
}

func TestInitRollupVersionResetKeepsHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	hourly, daily := retainedStore(t, path)
	rawExec(t, path, `PRAGMA user_version = 0`) // what .dump / .recover leave behind
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if v := scalarInt(t, store.db, `PRAGMA user_version`); v != rollupSchemaVersion {
		t.Fatalf("user_version = %d", v)
	}
	if !reflect.DeepEqual(hourly, rollupDump(t, store, "usage_hourly")) || !reflect.DeepEqual(daily, rollupDump(t, store, "usage_daily")) {
		t.Fatal("rollups were wiped or rebuilt")
	}
	// A rebuild from the kept details would have lost days 1-2.
	if rolled, kept := scalarInt(t, store.db, `SELECT SUM(requests) FROM usage_daily`), scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`); rolled <= kept {
		t.Fatalf("rollup requests %d <= kept details %d", rolled, kept)
	}
}

func TestInitRollupRefusesDataWithoutTriggers(t *testing.T) {
	for name, drop := range map[string][]string{
		"both triggers":  {`DROP TRIGGER usage_records_rollup_ai`, `DROP TRIGGER usage_records_rollup_ad`},
		"delete trigger": {`DROP TRIGGER usage_records_rollup_ad`},
		"insert trigger": {`DROP TRIGGER usage_records_rollup_ai`},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "usage.db")
			hourly, daily := retainedStore(t, path)
			rawExec(t, path, append(drop, `PRAGMA user_version = 0`)...)
			store, err := NewSQLiteStore(path)
			if err == nil {
				_ = store.Close()
				t.Fatal("NewSQLiteStore succeeded with rollup data and missing triggers")
			}
			if !strings.Contains(err.Error(), "trigger") {
				t.Fatalf("error does not mention triggers: %v", err)
			}
			if got := rawStrings(t, path, dumpHourly); !reflect.DeepEqual(got, hourly) {
				t.Fatal("hourly rollup touched")
			}
			if got := rawStrings(t, path, dumpDaily); !reflect.DeepEqual(got, daily) {
				t.Fatal("daily rollup touched")
			}
			if v := rawStrings(t, path, `PRAGMA user_version`); v[0] != "0" {
				t.Fatalf("user_version = %v after refusal", v)
			}
		})
	}
}

func TestInitRollupEmptyTablesWithoutTriggersBackfills(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	insertBulk(t, store, randomRecords(rand.New(rand.NewSource(12)), utc(1, 0, 0, 0), 3, 300))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	rawExec(t, path, `DROP TRIGGER usage_records_rollup_ai`, `DROP TRIGGER usage_records_rollup_ad`,
		`DELETE FROM usage_hourly`, `DELETE FROM usage_daily`, `PRAGMA user_version = 0`)
	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	assertRollupsMatchDetails(t, store)
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_daily`); n == 0 {
		t.Fatal("nothing backfilled")
	}
	if n := scalarInt(t, store.db, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'usage_records_rollup_%'`); n != 2 {
		t.Fatalf("triggers = %d", n)
	}
	if v := scalarInt(t, store.db, `PRAGMA user_version`); v != rollupSchemaVersion {
		t.Fatalf("user_version = %d", v)
	}
}

func TestDeleteBeforeRefusesWithoutControlRow(t *testing.T) {
	store := newTestStore(t)
	insertBulk(t, store, randomRecords(rand.New(rand.NewSource(13)), utc(1, 0, 0, 0), 4, 400))
	hourly, daily := rollupDump(t, store, "usage_hourly"), rollupDump(t, store, "usage_daily")
	before := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`)
	if _, err := store.db.Exec(`DELETE FROM usage_rollup_control`); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteBefore(context.Background(), utc(10, 0, 0, 0))
	if err == nil || !strings.Contains(err.Error(), "control row") {
		t.Fatalf("DeleteBefore = %d, %v", deleted, err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d", deleted)
	}
	if after := scalarInt(t, store.db, `SELECT COUNT(*) FROM usage_records`); after != before {
		t.Fatalf("details %d -> %d", before, after)
	}
	if !reflect.DeepEqual(hourly, rollupDump(t, store, "usage_hourly")) || !reflect.DeepEqual(daily, rollupDump(t, store, "usage_daily")) {
		t.Fatal("rollups changed")
	}
}

func TestAccountSummaryZeroTimeIsOpenBound(t *testing.T) {
	store := newTestStore(t)
	insertBulk(t, store, randomRecords(rand.New(rand.NewSource(14)), utc(1, 0, 0, 0), 3, 500))
	var zero time.Time
	mid := tp(utc(2, 7, 30, 0))
	for name, c := range map[string]struct{ got, want QueryRange }{
		"zero start":      {QueryRange{Start: &zero}, QueryRange{}},
		"zero end":        {QueryRange{End: &zero}, QueryRange{}},
		"both zero":       {QueryRange{Start: &zero, End: &zero}, QueryRange{}},
		"zero start, end": {QueryRange{Start: &zero, End: mid}, QueryRange{End: mid}},
		"start, zero end": {QueryRange{Start: mid, End: &zero}, QueryRange{Start: mid}},
	} {
		got, err := store.AccountSummary(context.Background(), c.got)
		if err != nil {
			t.Fatal(name, err)
		}
		want, err := store.AccountSummary(context.Background(), c.want)
		if err != nil {
			t.Fatal(name, err)
		}
		if len(want.Accounts) == 0 || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %+v\nwant %+v", name, got.Totals, want.Totals)
		}
	}
}

func TestWriterPragmasAndImmediateTx(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if n := scalarInt(t, store.db, `PRAGMA journal_size_limit`); n != 8388608 {
		t.Fatalf("journal_size_limit = %d", n)
	}
	insertAll(t, store, Record{ID: "w", Timestamp: utc(10, 1, 0, 0), AuthIndex: "a"})

	// _txlock=immediate: BEGIN already holds the write lock, before any write.
	other, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	conn, err := other.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err == nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		_ = tx.Rollback()
		t.Fatal("another connection took the write lock while the writer tx was open")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("write lock not released: %v", err)
	}
	_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
}

func TestConfigureDataDirChangeRestartsCleanup(t *testing.T) {
	lifecycle(t)
	now := time.Now()
	seedDir := func() string {
		dir := t.TempDir()
		seed, err := NewSQLiteStore(filepath.Join(dir, "usage.db"))
		if err != nil {
			t.Fatal(err)
		}
		insertAll(t, seed,
			Record{ID: "old", Timestamp: now.Add(-10 * 24 * time.Hour), AuthIndex: "a", Model: "gone"},
			Record{ID: "new", Timestamp: now.Add(-time.Hour), AuthIndex: "a", Model: "kept"})
		if err := seed.Close(); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	dirA, dirB := seedDir(), seedDir()
	configure := func(method, dir string) {
		t.Helper()
		errc := make(chan error, 1)
		go func() {
			raw, _ := json.Marshal(map[string]any{"config_yaml": "data_dir: '" + dir + "'\nretention_days: 1\n"})
			_, err := handleMethod(method, raw)
			errc <- err
		}()
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("%s %s: %v", method, dir, err)
			}
		case <-time.After(15 * time.Second): // timeout guard only: a deadlock
			t.Fatalf("%s %s did not return", method, dir)
		}
	}
	oldDone := func() chan struct{} { return cleanupChan() }
	cleaned := func(s *SQLiteStore) bool {
		return scalarInt(t, s.db, `SELECT COUNT(*) FROM usage_records WHERE id = 'old'`) == 0
	}

	configure("plugin.register", dirA)
	storeA, doneA := currentStore(), oldDone()
	pollUntil(t, "cleanup in dir A", func() bool { return cleaned(storeA) })

	configure("plugin.reconfigure", dirB)
	storeB, doneB := currentStore(), oldDone()
	if storeB == nil || storeB == storeA {
		t.Fatal("store not replaced after data_dir change")
	}
	select {
	case <-doneA:
	default:
		t.Fatal("old cleanup goroutine still running after reconfigure")
	}
	if doneB == nil || doneB == doneA {
		t.Fatal("cleanup not restarted for the new store")
	}
	pollUntil(t, "cleanup in dir B", func() bool { return cleaned(storeB) })
	if scalarInt(t, storeB.db, `SELECT COUNT(*) FROM usage_records WHERE id = 'new'`) != 1 {
		t.Fatal("recent row deleted")
	}
	// The old store is closed.
	if err := storeA.db.Ping(); err == nil {
		t.Fatal("old store still open")
	}
}
