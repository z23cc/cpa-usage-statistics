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
	}
	wantB := AccountUsage{
		AuthIndex: "idx-b", AuthID: "id-b", Source: "src-b", Provider: "codex", AuthType: "apikey",
		Requests: 2, Success: 1, Failed: 1, Tokens: tok(3, 3, 3, 3, 3, 3, 5007),
		FirstRequestAt: base.Add(4 * time.Minute), LastRequestAt: base.Add(5 * time.Minute),
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
	const tail = " ORDER BY timestamp DESC, id DESC LIMIT ?"
	cases := map[string]struct {
		where string
		args  []any
	}{
		"with auth_index":    {"auth_index = ? AND (timestamp, id) < (?, ?)", []any{"idx-1", cursorTS, "p120", 8}},
		"without auth_index": {"(timestamp, id) < (?, ?)", []any{cursorTS, "p120", 8}},
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
