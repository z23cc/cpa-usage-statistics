package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func insertAll(t *testing.T, store *SQLiteStore, recs ...Record) {
	t.Helper()
	for _, r := range recs {
		if err := store.Insert(context.Background(), r); err != nil {
			t.Fatalf("Insert %s: %v", r.ID, err)
		}
	}
}

func tok(in, out, reasoning, cached, cacheRead, cacheCreate, total int64) TokenStats {
	return TokenStats{in, out, reasoning, cached, cacheRead, cacheCreate, total}
}

func TestAccountSummary(t *testing.T) {
	store := newTestStore(t)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	insertAll(t, store,
		Record{ID: "a1", Timestamp: base.Add(1 * time.Minute), AuthIndex: "idx-a", AuthID: "id-a", Source: "src-a", Model: "m1", Provider: "claude", AuthType: "oauth", Tokens: tok(1, 2, 3, 4, 5, 6, 10)},
		Record{ID: "a2", Timestamp: base.Add(3 * time.Minute), AuthIndex: "idx-a", AuthID: "id-a", Source: "src-a", Model: "m1", Provider: "claude", AuthType: "oauth", Tokens: tok(10, 20, 30, 40, 50, 60, 100), Failed: true},
		Record{ID: "a3", Timestamp: base.Add(2 * time.Minute), AuthIndex: "idx-a", AuthID: "id-a", Source: "src-a", Model: "m1", Provider: "claude", AuthType: "oauth", Tokens: tok(100, 200, 300, 400, 500, 600, 1000)},
		Record{ID: "b1", Timestamp: base.Add(5 * time.Minute), AuthIndex: "idx-b", AuthID: "id-b", Source: "src-b", Model: "m1", Provider: "codex", AuthType: "apikey", Tokens: tok(1, 1, 1, 1, 1, 1, 5000)},
		Record{ID: "b2", Timestamp: base.Add(4 * time.Minute), AuthIndex: "idx-b", AuthID: "id-b", Source: "src-b", Model: "m1", Provider: "codex", AuthType: "apikey", Tokens: tok(2, 2, 2, 2, 2, 2, 7), Failed: true},
	)
	got, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatalf("AccountSummary: %v", err)
	}
	if len(got.Accounts) != 2 {
		t.Fatalf("accounts = %+v", got.Accounts)
	}
	// idx-b has more total tokens (5007 vs 1110) so it sorts first.
	b, a := got.Accounts[0], got.Accounts[1]
	if b.AuthIndex != "idx-b" || a.AuthIndex != "idx-a" {
		t.Fatalf("order = %s, %s", b.AuthIndex, a.AuthIndex)
	}
	wantA := AccountUsage{
		AuthIndex: "idx-a", AuthID: "id-a", Source: "src-a", Provider: "claude", AuthType: "oauth",
		Requests: 3, Success: 2, Failed: 1, Tokens: tok(111, 222, 333, 444, 555, 666, 1110),
		FirstRequestAt: base.Add(1 * time.Minute), LastRequestAt: base.Add(3 * time.Minute),
		Models: []ModelUsage{{Model: "m1", Requests: 3, Success: 2, Failed: 1, Tokens: tok(111, 222, 333, 444, 555, 666, 1110)}},
	}
	wantB := AccountUsage{
		AuthIndex: "idx-b", AuthID: "id-b", Source: "src-b", Provider: "codex", AuthType: "apikey",
		Requests: 2, Success: 1, Failed: 1, Tokens: tok(3, 3, 3, 3, 3, 3, 5007),
		FirstRequestAt: base.Add(4 * time.Minute), LastRequestAt: base.Add(5 * time.Minute),
		Models: []ModelUsage{{Model: "m1", Requests: 2, Success: 1, Failed: 1, Tokens: tok(3, 3, 3, 3, 3, 3, 5007)}},
	}
	for _, c := range []struct{ got, want AccountUsage }{{a, wantA}, {b, wantB}} {
		if !c.got.FirstRequestAt.Equal(c.want.FirstRequestAt) || !c.got.LastRequestAt.Equal(c.want.LastRequestAt) ||
			c.got.FirstRequestAt.Location() != time.UTC || c.got.LastRequestAt.Location() != time.UTC {
			t.Errorf("%s times = %v..%v", c.want.AuthIndex, c.got.FirstRequestAt, c.got.LastRequestAt)
		}
		c.got.FirstRequestAt, c.got.LastRequestAt = time.Time{}, time.Time{}
		c.want.FirstRequestAt, c.want.LastRequestAt = time.Time{}, time.Time{}
		c.want.UnpricedModels = []string{"m1"} // no prices configured
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("account = %+v\nwant     %+v", c.got, c.want)
		}
	}
	wantTotals := UsageTotals{Accounts: 2, Requests: 5, Success: 3, Failed: 2, Tokens: tok(114, 225, 336, 447, 558, 669, 6117)}
	if got.Totals != wantTotals {
		t.Fatalf("totals = %+v want %+v", got.Totals, wantTotals)
	}
}

func TestAccountSummaryOrderTieBreak(t *testing.T) {
	store := newTestStore(t)
	ts := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	insertAll(t, store,
		Record{ID: "1", Timestamp: ts, AuthIndex: "i1", AuthID: "z", Tokens: tok(0, 0, 0, 0, 0, 0, 5)},
		Record{ID: "2", Timestamp: ts, AuthIndex: "i2", AuthID: "a", Tokens: tok(0, 0, 0, 0, 0, 0, 5)},
		Record{ID: "3", Timestamp: ts, AuthIndex: "i0", AuthID: "a", Tokens: tok(0, 0, 0, 0, 0, 0, 5)},
	)
	got, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, a := range got.Accounts {
		order = append(order, a.AuthID+"/"+a.AuthIndex)
	}
	if strings.Join(order, ",") != "a/i0,a/i2,z/i1" {
		t.Fatalf("order = %v", order)
	}
}

func TestAccountSummaryRange(t *testing.T) {
	store := newTestStore(t)
	start := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC)
	insertAll(t, store,
		Record{ID: "before", Timestamp: start.Add(-time.Nanosecond), AuthIndex: "x", Tokens: tok(0, 0, 0, 0, 0, 0, 1)},
		Record{ID: "at-start", Timestamp: start, AuthIndex: "x", Tokens: tok(0, 0, 0, 0, 0, 0, 10)},
		Record{ID: "inside", Timestamp: start.Add(time.Minute), AuthIndex: "x", Tokens: tok(0, 0, 0, 0, 0, 0, 100)},
		Record{ID: "at-end", Timestamp: end, AuthIndex: "x", Tokens: tok(0, 0, 0, 0, 0, 0, 1000)},
		Record{ID: "after", Timestamp: end.Add(time.Hour), AuthIndex: "x", Tokens: tok(0, 0, 0, 0, 0, 0, 10000)},
	)
	got, err := store.AccountSummary(context.Background(), QueryRange{Start: &start, End: &end})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Accounts) != 1 || got.Accounts[0].Requests != 2 || got.Accounts[0].Tokens.TotalTokens != 110 {
		t.Fatalf("range = %+v", got.Accounts)
	}
	if !got.Accounts[0].FirstRequestAt.Equal(start) {
		t.Fatalf("first = %v", got.Accounts[0].FirstRequestAt)
	}
}

func TestAccountSummaryEmptyJSON(t *testing.T) {
	store := newTestStore(t)
	got, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Accounts == nil || len(got.Accounts) != 0 || got.Totals.Accounts != 0 {
		t.Fatalf("empty = %+v", got)
	}
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), `"accounts":[]`) {
		t.Fatalf("json = %s", raw)
	}
}

// seedRecords inserts 25 records across two auth_index values; ids are
// zero-padded, and timestamps repeat in groups of three so ties are exercised.
func seedRecords(t *testing.T, store *SQLiteStore) (base time.Time, recs []Record) {
	t.Helper()
	base = time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 25; i++ {
		idx, id := "idx-a", "id-a"
		if i%2 == 1 {
			idx, id = "idx-b", "id-b"
		}
		recs = append(recs, Record{
			ID: fmt.Sprintf("r%02d", i), Timestamp: base.Add(time.Duration(i/3) * time.Minute),
			AuthIndex: idx, AuthID: id, Model: "m",
		})
	}
	insertAll(t, store, recs...)
	return base, recs
}

func pageAll(t *testing.T, store *SQLiteStore, q RecordsQuery) []RequestDetail {
	t.Helper()
	var all []RequestDetail
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("paging does not terminate")
		}
		page, err := store.ListRecords(context.Background(), q)
		if err != nil {
			t.Fatalf("ListRecords: %v", err)
		}
		if len(page.Records) > q.Limit {
			t.Fatalf("page too large: %d", len(page.Records))
		}
		for _, r := range page.Records {
			all = append(all, r.RequestDetail)
		}
		if page.NextCursor == nil {
			return all
		}
		if len(page.Records) != q.Limit {
			t.Fatalf("non-final page has %d records", len(page.Records))
		}
		q.Cursor = *page.NextCursor
	}
}

func assertStrictlyDescending(t *testing.T, got []RequestDetail) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		p, c := got[i-1], got[i]
		if !(p.Timestamp.After(c.Timestamp) || (p.Timestamp.Equal(c.Timestamp) && p.ID > c.ID)) {
			t.Fatalf("order violated at %d: %s@%v then %s@%v", i, p.ID, p.Timestamp, c.ID, c.Timestamp)
		}
	}
}

func TestListRecordsPagingExactlyOnce(t *testing.T) {
	store := newTestStore(t)
	_, recs := seedRecords(t, store)
	got := pageAll(t, store, RecordsQuery{Limit: 7})
	if len(got) != len(recs) {
		t.Fatalf("got %d records want %d", len(got), len(recs))
	}
	seen := map[string]bool{}
	for _, d := range got {
		if seen[d.ID] {
			t.Fatalf("duplicate %s", d.ID)
		}
		seen[d.ID] = true
	}
	for _, r := range recs {
		if !seen[r.ID] {
			t.Fatalf("missing %s", r.ID)
		}
	}
	assertStrictlyDescending(t, got)
}

func TestListRecordsExactMultipleLastPage(t *testing.T) {
	store := newTestStore(t)
	seedRecords(t, store)
	// 25 records, limit 25: single page, no cursor.
	page, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 25})
	if err != nil || len(page.Records) != 25 || page.NextCursor != nil {
		t.Fatalf("page = %d next=%v err=%v", len(page.Records), page.NextCursor, err)
	}
	// limit 24: one more record remains.
	page, err = store.ListRecords(context.Background(), RecordsQuery{Limit: 24})
	if err != nil || len(page.Records) != 24 || page.NextCursor == nil {
		t.Fatalf("page = %d next=%v err=%v", len(page.Records), page.NextCursor, err)
	}
	page, err = store.ListRecords(context.Background(), RecordsQuery{Limit: 24, Cursor: *page.NextCursor})
	if err != nil || len(page.Records) != 1 || page.NextCursor != nil {
		t.Fatalf("last page = %d next=%v err=%v", len(page.Records), page.NextCursor, err)
	}
}

func TestListRecordsFilters(t *testing.T) {
	store := newTestStore(t)
	seedRecords(t, store)
	byIndex := pageAll(t, store, RecordsQuery{Limit: 4, AuthIndex: "idx-a"})
	if len(byIndex) != 13 {
		t.Fatalf("idx-a records = %d", len(byIndex))
	}
	for _, d := range byIndex {
		if d.AuthIndex != "idx-a" {
			t.Fatalf("leaked %+v", d)
		}
	}
	assertStrictlyDescending(t, byIndex)
	byID := pageAll(t, store, RecordsQuery{Limit: 5, AuthID: "id-b"})
	if len(byID) != 12 {
		t.Fatalf("id-b records = %d", len(byID))
	}
	for _, d := range byID {
		if d.AuthID != "id-b" {
			t.Fatalf("leaked %+v", d)
		}
	}
	both := pageAll(t, store, RecordsQuery{Limit: 5, AuthID: "id-b", AuthIndex: "idx-a"})
	if len(both) != 0 {
		t.Fatalf("conflicting filters returned %d", len(both))
	}
}

func TestListRecordsRangeWithCursor(t *testing.T) {
	store := newTestStore(t)
	base, _ := seedRecords(t, store)
	start := base.Add(2 * time.Minute) // minutes 2..5 -> i in 6..17? end exclusive
	end := base.Add(6 * time.Minute)
	got := pageAll(t, store, RecordsQuery{Limit: 3, Range: QueryRange{Start: &start, End: &end}})
	// minutes 2,3,4,5 hold i=6..17 (12 records)
	if len(got) != 12 {
		t.Fatalf("got %d", len(got))
	}
	for _, d := range got {
		if d.Timestamp.Before(start) || !d.Timestamp.Before(end) {
			t.Fatalf("out of range: %v", d.Timestamp)
		}
	}
	assertStrictlyDescending(t, got)
}

func TestListRecordsInvalidCursor(t *testing.T) {
	store := newTestStore(t)
	seedRecords(t, store)
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	goodTS := time.Now().UTC().Format(sqliteTimestampLayout)
	for name, cursor := range map[string]string{
		"not base64url": "!!!not base64!!!",
		"no separator":  enc("nopipe"),
		"empty id":      enc(goodTS + "|"),
		"bad timestamp": enc("yesterday|r01"),
	} {
		_, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 5, Cursor: cursor})
		if !errors.Is(err, errInvalidCursor) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestListRecordsBadLimitAndEmpty(t *testing.T) {
	store := newTestStore(t)
	for _, l := range []int{0, -1} {
		if _, err := store.ListRecords(context.Background(), RecordsQuery{Limit: l}); err == nil {
			t.Errorf("limit %d: expected error", l)
		}
	}
	page, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 5})
	if err != nil || page.Records == nil || len(page.Records) != 0 || page.NextCursor != nil {
		t.Fatalf("empty page = %+v err=%v", page, err)
	}
	raw, _ := json.Marshal(page)
	if string(raw) != `{"records":[],"next_cursor":null}` {
		t.Fatalf("json = %s", raw)
	}
}

// --- handlers ---

func useStore(t *testing.T, store *SQLiteStore) {
	t.Helper()
	storeMu.Lock()
	prevStore, prevPath := globalStore, currentDBPath
	globalStore, currentDBPath = store, ""
	storeMu.Unlock()
	t.Cleanup(func() {
		storeMu.Lock()
		globalStore, currentDBPath = prevStore, prevPath
		storeMu.Unlock()
	})
}

func callManagement(t *testing.T, method, path, rawQuery string) managementResponse {
	t.Helper()
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	reqRaw, _ := json.Marshal(managementRequest{Method: method, Path: path, Query: q})
	out, err := handleManagement(reqRaw)
	if err != nil {
		t.Fatalf("handleManagement: %v", err)
	}
	var env struct {
		OK     bool               `json:"ok"`
		Result managementResponse `json:"result"`
	}
	if err := json.Unmarshal(out, &env); err != nil || !env.OK {
		t.Fatalf("envelope = %s err=%v", out, err)
	}
	return env.Result
}

const (
	hostAccounts = "/v0/management" + managementAccountsPath
	hostRecords  = "/v0/management" + managementRecordsPath
	hostUsage    = "/v0/management" + managementUsagePath
)

func TestHandleAccounts(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	seedRecords(t, store)
	resp := callManagement(t, "GET", hostAccounts, "")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d body %s", resp.StatusCode, resp.Body)
	}
	var got AccountsResponse
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Accounts) != 2 || got.Totals.Accounts != 2 || got.Totals.Requests != 25 {
		t.Fatalf("accounts = %+v", got)
	}
	if r := callManagement(t, "GET", hostAccounts, "start=nope"); r.StatusCode != 400 {
		t.Fatalf("invalid start status = %d", r.StatusCode)
	}
	if r := callManagement(t, "GET", hostAccounts, "end=nope"); r.StatusCode != 400 {
		t.Fatalf("invalid end status = %d", r.StatusCode)
	}
	if r := callManagement(t, "DELETE", hostAccounts, ""); r.StatusCode != 405 {
		t.Fatalf("DELETE accounts status = %d", r.StatusCode)
	}
}

func decodePage(t *testing.T, resp managementResponse) RecordsPage {
	t.Helper()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d body %s", resp.StatusCode, resp.Body)
	}
	var page RecordsPage
	if err := json.Unmarshal(resp.Body, &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func TestHandleRecordsLimitAndCursor(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	var recs []Record
	for i := 0; i < 1005; i++ {
		recs = append(recs, Record{ID: fmt.Sprintf("r%04d", i), Timestamp: base.Add(time.Duration(i) * time.Second), AuthIndex: "i"})
	}
	insertAll(t, store, recs...)

	page := decodePage(t, callManagement(t, "GET", hostRecords, ""))
	if len(page.Records) != 100 || page.NextCursor == nil {
		t.Fatalf("default limit: %d next=%v", len(page.Records), page.NextCursor)
	}
	page = decodePage(t, callManagement(t, "GET", hostRecords, "limit=5000"))
	if len(page.Records) != 1000 || page.NextCursor == nil {
		t.Fatalf("clamped limit: %d", len(page.Records))
	}
	page = decodePage(t, callManagement(t, "GET", hostRecords, "limit=3&auth_index=i"))
	if len(page.Records) != 3 || page.NextCursor == nil || page.Records[0].ID != "r1004" {
		t.Fatalf("limit 3: %+v", page.Records)
	}
	page2 := decodePage(t, callManagement(t, "GET", hostRecords, "limit=3&auth_index=i&cursor="+url.QueryEscape(*page.NextCursor)))
	if len(page2.Records) != 3 || page2.Records[0].ID != "r1001" {
		t.Fatalf("page 2: %+v", page2.Records)
	}

	for _, bad := range []string{"abc", "0", "-3", "1.5"} {
		r := callManagement(t, "GET", hostRecords, "limit="+bad)
		if r.StatusCode != 400 || strings.TrimSpace(string(r.Body)) != `{"error":"invalid limit"}` {
			t.Errorf("limit=%s: %d %s", bad, r.StatusCode, r.Body)
		}
	}
	r := callManagement(t, "GET", hostRecords, "cursor=%21%21%21")
	if r.StatusCode != 400 || strings.TrimSpace(string(r.Body)) != `{"error":"invalid cursor"}` {
		t.Errorf("bad cursor: %d %s", r.StatusCode, r.Body)
	}
	if r := callManagement(t, "GET", hostRecords, "start=nope"); r.StatusCode != 400 {
		t.Errorf("invalid start status = %d", r.StatusCode)
	}
	if r := callManagement(t, "POST", hostRecords, ""); r.StatusCode != 405 {
		t.Errorf("POST records status = %d", r.StatusCode)
	}
}

func TestHandleUsageRouteUnchanged(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	insertAll(t, store, Record{ID: "u1", Timestamp: time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC), APIKey: "key-1", Model: "model-1", AuthIndex: "i"})
	resp := callManagement(t, "GET", hostUsage, "")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d body %s", resp.StatusCode, resp.Body)
	}
	var usage APIUsage
	if err := json.Unmarshal(resp.Body, &usage); err != nil {
		t.Fatal(err)
	}
	if d := usage["key-1"]["model-1"]; len(d) != 1 || d[0].ID != "u1" {
		t.Fatalf("usage = %s", resp.Body)
	}
}

func TestManagementRegistrationRoutes(t *testing.T) {
	routes := managementRegistration().Routes
	want := map[string]bool{
		"GET " + managementUsagePath: true, "DELETE " + managementUsagePath: true,
		"GET " + managementAccountsPath: true, "GET " + managementRecordsPath: true,
		"GET " + managementPricesPath: true, "PUT " + managementPricesPath: true,
		"GET " + managementHealthPath: true,
	}
	if len(routes) != len(want) {
		t.Fatalf("routes = %+v", routes)
	}
	for _, r := range routes {
		if !want[r.Method+" "+r.Path] {
			t.Errorf("unexpected route %+v", r)
		}
	}
	res := managementRegistration().Resources
	if len(res) != 1 || res[0].Path != "/ui" || res[0].Menu != "账号用量" {
		t.Errorf("resources = %+v", res)
	}
	raw, _ := json.Marshal(managementRegistration())
	if !strings.Contains(string(raw), `"resources":[`) {
		t.Errorf("json lacks resources: %s", raw)
	}
}

// --- schema ---

func TestSchemaCompatibility(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	rows, err := store.db.QueryContext(ctx, `PRAGMA table_info(usage_records)`)
	if err != nil {
		t.Fatal(err)
	}
	var cols []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, name)
	}
	_ = rows.Close()
	want := "id,timestamp,api_key,provider,model,alias,source,auth_id,auth_index,auth_type,executor_type," +
		"reasoning_effort,service_tier,latency_ms,ttft_ms,input_tokens,output_tokens,reasoning_tokens," +
		"cached_tokens,cache_read_tokens,cache_creation_tokens,total_tokens,failed,failure_status_code,failure_body"
	if strings.Join(cols, ",") != want {
		t.Fatalf("columns = %v", cols)
	}
	var n int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_usage_records_auth_ts'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("auth_ts index count = %d err=%v", n, err)
	}
	var timeout int
	if err := store.db.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&timeout); err != nil || timeout != 3000 {
		t.Fatalf("busy_timeout = %d err=%v", timeout, err)
	}
}

// --- follow-up: query plans, strict cursor, tie-break, golden ---

func explainPlan(t *testing.T, store *SQLiteStore, query string, args ...any) string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	return strings.Join(details, "\n")
}

// The SQL below mirrors ListRecords' page-2 query; keep in sync with store.go.
func TestListRecordsQueryPlan(t *testing.T) {
	store := newTestStore(t)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	var recs []Record
	for i := 0; i < 300; i++ {
		recs = append(recs, Record{
			ID: fmt.Sprintf("p%03d", i), Timestamp: base.Add(time.Duration(i/2) * time.Second),
			AuthIndex: fmt.Sprintf("idx-%d", i%3),
		})
	}
	insertAll(t, store, recs...)
	if _, err := store.db.Exec(`ANALYZE`); err != nil {
		t.Fatal(err)
	}
	cursorTS := formatTimestamp(base.Add(60 * time.Second))
	const tail = " ORDER BY timestamp DESC, id DESC LIMIT ? OFFSET ?"
	cases := map[string]struct {
		where string
		args  []any
	}{
		"with auth_index":    {"auth_index = ? AND (timestamp, id) < (?, ?)", []any{"idx-1", cursorTS, "p120", 8, 0}},
		"without auth_index": {"(timestamp, id) < (?, ?)", []any{cursorTS, "p120", 8, 0}},
	}
	for name, c := range cases {
		plan := explainPlan(t, store, "SELECT "+recordColumns+" FROM usage_records WHERE "+c.where+tail, c.args...)
		if strings.Contains(plan, "MULTI-INDEX OR") || strings.Contains(plan, "USE TEMP B-TREE FOR ORDER BY") {
			t.Errorf("%s: bad plan:\n%s", name, plan)
		}
		if name == "with auth_index" && !strings.Contains(plan, "SEARCH usage_records USING INDEX idx_usage_records_auth_ts") {
			t.Errorf("%s: auth_ts index not searched:\n%s", name, plan)
		}
	}

	// Page mode and the filters added with it: all must stay on the auth_ts index.
	rng := "timestamp >= ? AND timestamp < ?"
	rngArgs := []any{formatTimestamp(base), formatTimestamp(base.Add(time.Hour))}
	strict := map[string]struct {
		sql  string
		args []any
	}{
		"auth_index+auth_id+model+failed offset": {
			"SELECT " + recordColumns + " FROM usage_records WHERE auth_index = ? AND auth_id = ? AND model = ? AND failed = ?" + tail,
			[]any{"idx-1", "id-1", "m1", 0, 8, 16},
		},
		"count": {
			"SELECT COUNT(*) FROM usage_records WHERE auth_index = ? AND " + rng,
			append([]any{"idx-1"}, rngArgs...),
		},
		"count with model+failed": {
			"SELECT COUNT(*) FROM usage_records WHERE auth_index = ? AND " + rng + " AND model = ? AND failed = ?",
			append(append([]any{"idx-1"}, rngArgs...), "m1", 0),
		},
	}
	for name, c := range strict {
		plan := explainPlan(t, store, c.sql, c.args...)
		if !strings.Contains(plan, "idx_usage_records_auth_ts") {
			t.Errorf("%s: auth_ts index unused:\n%s", name, plan)
		}
		if strings.Contains(plan, "USE TEMP B-TREE FOR ORDER BY") || strings.Contains(plan, "SCAN usage_records") {
			t.Errorf("%s: bad plan:\n%s", name, plan)
		}
	}
}

func TestListRecordsCursorRejectsNonCanonicalTimestamp(t *testing.T) {
	store := newTestStore(t)
	seedRecords(t, store)
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for name, ts := range map[string]string{
		"offset":         "2026-10-22T14:00:00.000000000+08:00",
		"fewer fraction": "2026-10-22T06:00:00Z",
		"short fraction": "2026-10-22T06:00:00.000Z",
		"lowercase z/t":  "2026-10-22t06:00:00.000000000z",
		"no zone offset": "2026-10-22T06:00:00.000000000",
	} {
		_, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 5, Cursor: enc(ts + "|r01")})
		if !errors.Is(err, errInvalidCursor) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	// Control: the canonical form is accepted.
	canon := formatTimestamp(time.Date(2026, 5, 2, 10, 5, 0, 0, time.UTC))
	if _, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 5, Cursor: enc(canon + "|r01")}); err != nil {
		t.Errorf("canonical cursor rejected: %v", err)
	}
}

func TestListRecordsTieBreakIndependentOfInsertOrder(t *testing.T) {
	store := newTestStore(t)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	// Groups of 4 equal timestamps; within a group ids are inserted in
	// descending lexical order, so rowid order is the reverse of id order.
	const groups, perGroup = 6, 4
	for g := 0; g < groups; g++ {
		for j := perGroup - 1; j >= 0; j-- {
			insertAll(t, store, Record{
				ID: fmt.Sprintf("g%d-%d", g, j), Timestamp: base.Add(time.Duration(g) * time.Minute),
				AuthIndex: fmt.Sprintf("idx-%d", j%2),
			})
		}
	}
	for _, limit := range []int{1, 3, 5} { // page boundaries fall inside tie groups
		got := pageAll(t, store, RecordsQuery{Limit: limit})
		if len(got) != groups*perGroup {
			t.Fatalf("limit %d: got %d", limit, len(got))
		}
		assertStrictlyDescending(t, got)
	}
	filtered := pageAll(t, store, RecordsQuery{Limit: 2, AuthIndex: "idx-1"})
	if len(filtered) != groups*perGroup/2 {
		t.Fatalf("filtered got %d", len(filtered))
	}
	assertStrictlyDescending(t, filtered)
}

func TestHandleUsageGolden(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	insertAll(t, store, Record{
		ID: "g1", Timestamp: time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC), APIKey: "k", Provider: "anthropic",
		Model: "m", Alias: "al", Source: "src", AuthID: "aid", AuthIndex: "aidx", AuthType: "oauth",
		ExecutorType: "ex", ReasoningEffort: "high", ServiceTier: "prio", LatencyMs: 1250, TTFTMs: 200,
		Tokens: tok(1, 2, 3, 4, 5, 6, 7), Failed: true, FailureStatusCode: 429, FailureBody: "boom",
	})
	resp := callManagement(t, "GET", hostUsage, "")
	const want = `{"k":{"m":[{"id":"g1","timestamp":"2026-05-02T10:30:00Z","provider":"anthropic","model":"m","alias":"al",` +
		`"source":"src","auth_id":"aid","auth_index":"aidx","auth_type":"oauth","executor_type":"ex",` +
		`"reasoning_effort":"high","service_tier":"prio","latency_ms":1250,"ttft_ms":200,` +
		`"tokens":{"input_tokens":1,"output_tokens":2,"reasoning_tokens":3,"cached_tokens":4,"cache_read_tokens":5,` +
		`"cache_creation_tokens":6,"total_tokens":7},"failed":true,"failure_status_code":429,"failure_body":"boom"}]}}`
	if resp.StatusCode != 200 || string(resp.Body) != want {
		t.Fatalf("status %d\n got %s\nwant %s", resp.StatusCode, resp.Body, want)
	}
}

// --- account models, numbered paging, record filters ---

func TestAccountSummaryModels(t *testing.T) {
	store := newTestStore(t)
	putPrices(t, store,
		ModelPrice{Model: "big", Input: 2, Output: 8, CacheRead: 0.5, CacheCreation: 1},
		ModelPrice{Model: "tie-b", Input: 2},
		ModelPrice{Model: "free"}, // priced at zero: cost is 0, not null
	)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	acct := func(id, model string, n int, failed bool, tk TokenStats) Record {
		return Record{ID: id, Timestamp: base.Add(time.Duration(n) * time.Minute), AuthIndex: "idx-x", AuthID: "id-x",
			Provider: "claude", Model: model, Tokens: tk, Failed: failed}
	}
	insertAll(t, store,
		acct("1", "free", 1, false, tok(0, 0, 0, 0, 0, 0, 5)),
		acct("2", "tie-b", 2, false, tok(1_000_000, 0, 0, 0, 0, 0, 100)),
		acct("3", "big", 3, false, tok(1_000_000, 250_000, 0, 100_000, 0, 50_000, 200)),
		acct("4", "big", 4, true, tok(1_000_000, 250_000, 0, 100_000, 0, 50_000, 100)),
		acct("5", "tie-a", 5, false, tok(7, 0, 0, 0, 0, 0, 100)),
		Record{ID: "o1", Timestamp: base, AuthIndex: "idx-y", AuthID: "id-y", Provider: "claude", Model: "big", Tokens: tok(10, 0, 0, 0, 0, 0, 3)},
	)
	got, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	var x AccountUsage
	for _, a := range got.Accounts {
		if a.AuthIndex == "idx-x" {
			x = a
		}
	}
	f := func(v float64) *float64 { return &v }
	want := []ModelUsage{
		{Model: "big", Requests: 2, Success: 1, Failed: 1, Tokens: tok(2_000_000, 500_000, 0, 200_000, 0, 100_000, 300), CostUSD: f(8.2)},
		{Model: "tie-a", Requests: 1, Success: 1, Tokens: tok(7, 0, 0, 0, 0, 0, 100)}, // unpriced
		{Model: "tie-b", Requests: 1, Success: 1, Tokens: tok(1_000_000, 0, 0, 0, 0, 0, 100), CostUSD: f(2)},
		{Model: "free", Requests: 1, Success: 1, Tokens: tok(0, 0, 0, 0, 0, 0, 5), CostUSD: f(0)},
	}
	if len(x.Models) != len(want) {
		t.Fatalf("models = %+v", x.Models)
	}
	var sum float64
	for i, w := range want {
		g := x.Models[i]
		if g.Model != w.Model || g.Requests != w.Requests || g.Success != w.Success || g.Failed != w.Failed || g.Tokens != w.Tokens {
			t.Errorf("models[%d] = %+v want %+v", i, g, w)
		}
		if (g.CostUSD == nil) != (w.CostUSD == nil) || (w.CostUSD != nil && !near(*g.CostUSD, *w.CostUSD)) {
			t.Errorf("models[%d] %s cost = %v want %v", i, w.Model, g.CostUSD, w.CostUSD)
		}
		if g.CostUSD != nil {
			sum += *g.CostUSD
		}
	}
	if !near(sum, x.CostUSD) || !near(x.CostUSD, 10.2) {
		t.Errorf("model cost sum %v, account cost %v", sum, x.CostUSD)
	}
	if !reflect.DeepEqual(x.UnpricedModels, []string{"tie-a"}) {
		t.Errorf("unpriced = %v", x.UnpricedModels)
	}
	raw, _ := json.Marshal(x.Models)
	if !strings.Contains(string(raw), `"model":"tie-a"`) || !strings.Contains(string(raw), `"cost_usd":null`) {
		t.Errorf("json = %s", raw)
	}
	for _, a := range got.Accounts {
		if a.AuthIndex == "idx-y" && (len(a.Models) != 1 || a.Models[0].Tokens.TotalTokens != 3 || a.Models[0].Requests != 1 || a.Models[0].CostUSD == nil || !near(*a.Models[0].CostUSD, 0.00002)) {
			t.Errorf("idx-y models = %+v", a.Models)
		}
	}
}

// seedFilterRecords inserts 30 records: timestamps repeat in groups of three,
// model m-x for i%3==0 else m-y, failed for i%4==0, idx-a for even i.
func seedFilterRecords(t *testing.T, store *SQLiteStore) (recs []Record) {
	t.Helper()
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		r := Record{ID: fmt.Sprintf("f%02d", i), Timestamp: base.Add(time.Duration(i/3) * time.Minute),
			AuthIndex: "idx-a", AuthID: "id-a", Model: "m-y", Failed: i%4 == 0}
		if i%2 == 1 {
			r.AuthIndex, r.AuthID = "idx-b", "id-b"
		}
		if i%3 == 0 {
			r.Model = "m-x"
		}
		recs = append(recs, r)
	}
	insertAll(t, store, recs...)
	return recs
}

// pageNumbered walks page=1.. until the end, checking total on every page.
func pageNumbered(t *testing.T, store *SQLiteStore, q RecordsQuery) (all []RequestDetail, total int64) {
	t.Helper()
	for p := 1; ; p++ {
		if p > 100 {
			t.Fatal("numbered paging does not terminate")
		}
		q.Page = p
		page, err := store.ListRecords(context.Background(), q)
		if err != nil {
			t.Fatalf("page %d: %v", p, err)
		}
		if page.Total == nil {
			t.Fatalf("page %d: total missing", p)
		}
		if p == 1 {
			total = *page.Total
		} else if *page.Total != total {
			t.Fatalf("page %d: total %d != %d", p, *page.Total, total)
		}
		if len(page.Records) > q.Limit {
			t.Fatalf("page %d too large: %d", p, len(page.Records))
		}
		if len(page.Records) == 0 {
			return all, total
		}
		for _, r := range page.Records {
			all = append(all, r.RequestDetail)
		}
	}
}

func ids(ds []RequestDetail) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}

func TestListRecordsPageMode(t *testing.T) {
	store := newTestStore(t)
	seedFilterRecords(t, store)
	ctx := context.Background()
	for _, c := range []struct {
		page, limit, want int
	}{{1, 7, 7}, {2, 7, 7}, {5, 7, 2}, {6, 7, 0}, {100, 7, 0}, {3, 10, 10}, {4, 10, 0}, {1, 30, 30}, {2, 30, 0}, {1, 1000, 30}} {
		page, err := store.ListRecords(ctx, RecordsQuery{Limit: c.limit, Page: c.page})
		if err != nil {
			t.Fatal(err)
		}
		if page.Records == nil || len(page.Records) != c.want || page.Total == nil || *page.Total != 30 {
			t.Errorf("page %d limit %d: %d records total=%v", c.page, c.limit, len(page.Records), page.Total)
		}
	}
	// Page 1 and 2 are the newest records, in order.
	p1, _ := store.ListRecords(ctx, RecordsQuery{Limit: 3, Page: 1})
	p2, _ := store.ListRecords(ctx, RecordsQuery{Limit: 3, Page: 2})
	if got := ids([]RequestDetail{p1.Records[0].RequestDetail, p1.Records[2].RequestDetail, p2.Records[0].RequestDetail}); strings.Join(got, ",") != "f29,f27,f26" {
		t.Errorf("order = %v", got)
	}
	// Empty store: total 0, empty records.
	empty := newTestStore(t)
	page, err := empty.ListRecords(ctx, RecordsQuery{Limit: 5, Page: 1})
	if err != nil || page.Total == nil || *page.Total != 0 || page.Records == nil || len(page.Records) != 0 {
		t.Errorf("empty = %+v err=%v", page, err)
	}
	// Cursor mode carries no total.
	page, _ = store.ListRecords(ctx, RecordsQuery{Limit: 5})
	if page.Total != nil {
		t.Errorf("cursor mode total = %d", *page.Total)
	}
}

func TestListRecordsPageMatchesCursorWalkAndFilters(t *testing.T) {
	store := newTestStore(t)
	recs := seedFilterRecords(t, store)
	yes, no := true, false
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	start, end := base.Add(2*time.Minute), base.Add(7*time.Minute)
	cases := map[string]struct {
		q     RecordsQuery
		match func(Record) bool
	}{
		"none":              {RecordsQuery{}, func(Record) bool { return true }},
		"model":             {RecordsQuery{Model: "m-x"}, func(r Record) bool { return r.Model == "m-x" }},
		"model no match":    {RecordsQuery{Model: "nope"}, func(Record) bool { return false }},
		"model exact":       {RecordsQuery{Model: "m"}, func(Record) bool { return false }},
		"failed true":       {RecordsQuery{Failed: &yes}, func(r Record) bool { return r.Failed }},
		"failed false":      {RecordsQuery{Failed: &no}, func(r Record) bool { return !r.Failed }},
		"model+failed":      {RecordsQuery{Model: "m-y", Failed: &no}, func(r Record) bool { return r.Model == "m-y" && !r.Failed }},
		"auth_index":        {RecordsQuery{AuthIndex: "idx-a"}, func(r Record) bool { return r.AuthIndex == "idx-a" }},
		"all filters":       {RecordsQuery{AuthIndex: "idx-a", AuthID: "id-a", Model: "m-x", Failed: &yes}, func(r Record) bool { return r.AuthIndex == "idx-a" && r.Model == "m-x" && r.Failed }},
		"range+failed true": {RecordsQuery{Range: QueryRange{Start: &start, End: &end}, Failed: &yes}, func(r Record) bool { return r.Failed && !r.Timestamp.Before(start) && r.Timestamp.Before(end) }},
	}
	for name, c := range cases {
		var want int
		for _, r := range recs {
			if c.match(r) {
				want++
			}
		}
		for _, limit := range []int{1, 4, 7, 100} {
			q := c.q
			q.Limit = limit
			cursor := pageAll(t, store, q)
			paged, total := pageNumbered(t, store, q)
			if len(cursor) != want || int(total) != want || len(paged) != want {
				t.Errorf("%s limit %d: cursor=%d paged=%d total=%d want %d", name, limit, len(cursor), len(paged), total, want)
				continue
			}
			if !reflect.DeepEqual(ids(cursor), ids(paged)) {
				t.Errorf("%s limit %d: cursor %v != paged %v", name, limit, ids(cursor), ids(paged))
			}
			assertStrictlyDescending(t, paged)
			byID := map[string]Record{}
			for _, r := range recs {
				byID[r.ID] = r
			}
			for _, d := range paged {
				if !c.match(byID[d.ID]) {
					t.Errorf("%s: leaked %s", name, d.ID)
				}
			}
		}
	}
}

func TestHandleRecordsPageAndFilters(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	seedFilterRecords(t, store)

	for query, want := range map[string]string{
		"failed=maybe":         `{"error":"invalid failed"}`,
		"page=0":               `{"error":"invalid page"}`,
		"page=-1":              `{"error":"invalid page"}`,
		"page=abc":             `{"error":"invalid page"}`,
		"page=1.5":             `{"error":"invalid page"}`,
		"page=1000001":         `{"error":"invalid page"}`,
		"page=abc&cursor=xyz":  `{"error":"invalid page"}`,
		"page=1&cursor=xyz":    `{"error":"page and cursor cannot be combined"}`,
		"page=2&cursor=%21%21": `{"error":"page and cursor cannot be combined"}`,
	} {
		r := callManagement(t, "GET", hostRecords, query)
		if r.StatusCode != 400 || strings.TrimSpace(string(r.Body)) != want {
			t.Errorf("%s: %d %s", query, r.StatusCode, r.Body)
		}
	}

	keys := func(r managementResponse) map[string]json.RawMessage {
		t.Helper()
		if r.StatusCode != 200 {
			t.Fatalf("status %d body %s", r.StatusCode, r.Body)
		}
		m := map[string]json.RawMessage{}
		if err := json.Unmarshal(r.Body, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if _, ok := keys(callManagement(t, "GET", hostRecords, "limit=5"))["total"]; ok {
		t.Error("cursor mode body has total key")
	}
	m := keys(callManagement(t, "GET", hostRecords, "limit=5&page=1"))
	if string(m["total"]) != "30" {
		t.Errorf("page mode total = %s", m["total"])
	}
	// Max page is accepted; far past the end is an empty page with the total.
	r := callManagement(t, "GET", hostRecords, "page=1000000&limit=1000")
	if page := decodePage(t, r); len(page.Records) != 0 || page.Total == nil || *page.Total != 30 {
		t.Errorf("max page = %s", r.Body)
	}

	// Filters through the handler, both modes.
	page := decodePage(t, callManagement(t, "GET", hostRecords, "model=m-x&failed=true&limit=100"))
	if len(page.Records) != 3 || page.Total != nil { // i = 0, 12, 24
		t.Errorf("cursor filter = %+v", page.Records)
	}
	for _, f := range []string{"true", "1", "T"} {
		page = decodePage(t, callManagement(t, "GET", hostRecords, "failed="+f+"&page=1&limit=5"))
		if page.Total == nil || *page.Total != 8 || len(page.Records) != 5 { // i%4==0 -> 8
			t.Errorf("failed=%s page = total %v n %d", f, page.Total, len(page.Records))
		}
	}
	page = decodePage(t, callManagement(t, "GET", hostRecords, "failed=false&model=m-y&page=2&limit=10"))
	if page.Total == nil || *page.Total != 15 || len(page.Records) != 5 {
		t.Errorf("failed=false page = total %v n %d", page.Total, len(page.Records))
	}
	page = decodePage(t, callManagement(t, "GET", hostRecords, "failed=&model=&page=1"))
	if page.Total == nil || *page.Total != 30 {
		t.Errorf("blank filters total = %v", page.Total)
	}
}

func TestHandleRecordsNilStore(t *testing.T) {
	useStore(t, nil)
	for query, want := range map[string]string{
		"page=1": `{"records":[],"next_cursor":null,"total":0}`,
		"":       `{"records":[],"next_cursor":null}`,
	} {
		r := callManagement(t, "GET", hostRecords, query)
		if r.StatusCode != 200 || strings.TrimSpace(string(r.Body)) != want {
			t.Errorf("%q: %d %s", query, r.StatusCode, r.Body)
		}
	}
}
