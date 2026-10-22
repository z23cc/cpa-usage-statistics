package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var testPrice = ModelPrice{Model: "priced", Input: 2, Output: 8, CacheRead: 0.5, CacheCreation: 1}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func putPrices(t *testing.T, store *SQLiteStore, prices ...ModelPrice) PricesResponse {
	t.Helper()
	valid, err := validatePrices(prices)
	if err != nil {
		t.Fatalf("validatePrices: %v", err)
	}
	if err := store.ReplacePrices(context.Background(), valid); err != nil {
		t.Fatalf("ReplacePrices: %v", err)
	}
	res, err := store.Prices(context.Background())
	if err != nil {
		t.Fatalf("Prices: %v", err)
	}
	return res
}

// --- prices ---

func TestPricesCRUD(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	insertAll(t, store,
		Record{ID: "1", Timestamp: time.Now(), Model: "zeta"},
		Record{ID: "2", Timestamp: time.Now(), Model: "alpha"},
		Record{ID: "3", Timestamp: time.Now(), Model: "alpha"},
	)
	empty, err := store.Prices(ctx)
	if err != nil || empty.Prices == nil || len(empty.Prices) != 0 {
		t.Fatalf("initial = %+v err=%v", empty, err)
	}
	if strings.Join(empty.ModelsSeen, ",") != "alpha,zeta" {
		t.Fatalf("models seen = %v", empty.ModelsSeen)
	}

	res := putPrices(t, store,
		ModelPrice{Model: "zeta", Input: 1, Output: 2, CacheRead: 3, CacheCreation: 4},
		ModelPrice{Model: "beta", Input: 5},
	)
	if len(res.Prices) != 2 || res.Prices[0].Model != "beta" || res.Prices[1].Model != "zeta" {
		t.Fatalf("sorted prices = %+v", res.Prices)
	}
	z := res.Prices[1]
	if z.Input != 1 || z.Output != 2 || z.CacheRead != 3 || z.CacheCreation != 4 || z.UpdatedAt.IsZero() {
		t.Fatalf("zeta = %+v", z)
	}
	// "beta" has a price but no usage: priced models need not be seen.
	if strings.Join(res.ModelsSeen, ",") != "alpha,zeta" {
		t.Fatalf("models seen = %v", res.ModelsSeen)
	}

	// Replacement drops entries that are not resent.
	res = putPrices(t, store, ModelPrice{Model: "alpha", Input: 9})
	if len(res.Prices) != 1 || res.Prices[0].Model != "alpha" {
		t.Fatalf("after replace = %+v", res.Prices)
	}

	// Persisted and reloaded into the cache by a fresh store.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if p, ok := store.priceFor("alpha"); !ok || p.Input != 9 {
		t.Fatalf("reloaded alpha = %+v ok=%v", p, ok)
	}
	if _, ok := store.priceFor("zeta"); ok {
		t.Fatal("zeta survived replace")
	}

	// An explicit empty list clears the table and the cache.
	res = putPrices(t, store)
	if res.Prices == nil || len(res.Prices) != 0 {
		t.Fatalf("cleared = %+v", res.Prices)
	}
	if _, ok := store.priceFor("alpha"); ok {
		t.Fatal("cache not cleared")
	}
}

func TestValidatePrices(t *testing.T) {
	long := strings.Repeat("x", 201)
	many := make([]ModelPrice, 501)
	for i := range many {
		many[i] = ModelPrice{Model: fmt.Sprintf("m%d", i)}
	}
	bad := map[string][]ModelPrice{
		"empty model":       {{Model: ""}},
		"blank model":       {{Model: "   "}},
		"long model":        {{Model: long}},
		"duplicate":         {{Model: "a"}, {Model: "a"}},
		"duplicate trimmed": {{Model: "a"}, {Model: " a "}},
		"NaN input":         {{Model: "a", Input: math.NaN()}},
		"NaN output":        {{Model: "a", Output: math.NaN()}},
		"NaN cache_read":    {{Model: "a", CacheRead: math.NaN()}},
		"NaN cache_create":  {{Model: "a", CacheCreation: math.NaN()}},
		"negative input":    {{Model: "a", Input: -0.01}},
		"negative output":   {{Model: "a", Output: -1}},
		"negative read":     {{Model: "a", CacheRead: -1}},
		"negative create":   {{Model: "a", CacheCreation: -1}},
		"input too big":     {{Model: "a", Input: 10000.01}},
		"output too big":    {{Model: "a", Output: 1e9}},
		"read too big":      {{Model: "a", CacheRead: 10001}},
		"create too big":    {{Model: "a", CacheCreation: 10001}},
		"too many":          many,
	}
	for name, in := range bad {
		if _, err := validatePrices(in); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	okIn := []ModelPrice{
		{Model: "  trimmed  ", Input: 10000, Output: 0, UpdatedAt: time.Now()},
		{Model: strings.Repeat("y", 200)},
	}
	out, err := validatePrices(okIn)
	if err != nil {
		t.Fatalf("valid rejected: %v", err)
	}
	if out[0].Model != "trimmed" || out[0].Input != 10000 || !out[0].UpdatedAt.IsZero() {
		t.Fatalf("normalized = %+v", out[0])
	}
	if _, err := validatePrices(many[:500]); err != nil {
		t.Fatalf("500 entries rejected: %v", err)
	}
	if out, err := validatePrices([]ModelPrice{}); err != nil || len(out) != 0 {
		t.Fatalf("empty list: %v %v", out, err)
	}
}

// --- cost ---

func TestCostFormula(t *testing.T) {
	price := ModelPrice{Input: 3, Output: 15, CacheRead: 0.3, CacheCreation: 3.75}
	// (1000*3 + 2000*0.3 + 400*3.75 + 500*15) / 1e6 = 12600 / 1e6
	if got := costUSD(price, 1000, 2000, 400, 500); !near(got, 0.0126) {
		t.Fatalf("costUSD = %v", got)
	}
	if got := costUSD(price, 0, 0, 0, 0); got != 0 {
		t.Fatalf("zero cost = %v", got)
	}

	cases := []struct {
		name     string
		provider string
		tokens   TokenStats
		want     int64
	}{
		{"openai subtracts cache", "openai", TokenStats{InputTokens: 10000, CachedTokens: 4000, CacheCreationTokens: 1000}, 5000},
		{"codex clamps at zero", "codex", TokenStats{InputTokens: 100, CachedTokens: 300}, 0},
		{"claude keeps input", "claude", TokenStats{InputTokens: 1000, CachedTokens: 5000, CacheCreationTokens: 700}, 1000},
		{"Anthropic case-insensitive", "Anthropic", TokenStats{InputTokens: 1000, CachedTokens: 500}, 1000},
		{"substring claude", "my-CLAUDE-proxy", TokenStats{InputTokens: 42, CachedTokens: 40}, 42},
		{"empty provider", "", TokenStats{InputTokens: 10, CachedTokens: 3, CacheCreationTokens: 2}, 5},
	}
	for _, c := range cases {
		if got := pureInputTokens(c.provider, c.tokens); got != c.want {
			t.Errorf("%s: pureInputTokens = %d want %d", c.name, got, c.want)
		}
	}

	if got := roundUSD(0.0012345674); got != 0.001235 {
		t.Errorf("roundUSD = %v", got)
	}
	if got := roundUSD(0.0000004); got != 0 {
		t.Errorf("roundUSD small = %v", got)
	}
}

func TestPureInputSQLMatchesGo(t *testing.T) {
	store := newTestStore(t)
	rows := []Record{
		{ID: "1", Provider: "openai", Tokens: TokenStats{InputTokens: 10000, CachedTokens: 4000, CacheCreationTokens: 1000, TotalTokens: 1}},
		{ID: "2", Provider: "codex", Tokens: TokenStats{InputTokens: 100, CachedTokens: 300}},
		{ID: "3", Provider: "claude", Tokens: TokenStats{InputTokens: 1000, CachedTokens: 5000, CacheCreationTokens: 700}},
		{ID: "4", Provider: "ANTHROPIC-x", Tokens: TokenStats{InputTokens: 7, CachedTokens: 9}},
		{ID: "5", Provider: "", Tokens: TokenStats{InputTokens: 10, CachedTokens: 3, CacheCreationTokens: 2}},
		{ID: "6", Provider: "gemini", Tokens: TokenStats{InputTokens: 50, CacheCreationTokens: 60}},
	}
	for i := range rows {
		rows[i].Timestamp = time.Now()
	}
	insertAll(t, store, rows...)
	for _, r := range rows {
		var got int64
		if err := store.db.QueryRow(`SELECT `+pureInputSQL+` FROM usage_records WHERE id = ?`, r.ID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if want := pureInputTokens(r.Provider, r.Tokens); got != want {
			t.Errorf("provider %q: SQL %d, Go %d", r.Provider, got, want)
		}
	}
}

func TestAccountSummaryCost(t *testing.T) {
	store := newTestStore(t)
	putPrices(t, store, testPrice)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	insertAll(t, store,
		// Account X (openai): priced model, two records.
		// r1: pure 5000*2 + 4000*0.5 + 1000*1 + 500*8 = 17000 -> 0.017
		Record{ID: "x1", Timestamp: base, AuthIndex: "ix", AuthID: "x", Provider: "openai", Model: "priced", Source: "sx",
			Tokens: TokenStats{InputTokens: 10000, CachedTokens: 4000, CacheCreationTokens: 1000, OutputTokens: 500, TotalTokens: 15500}},
		// r2: 2000*2 + 100*8 = 4800 -> 0.0048
		Record{ID: "x2", Timestamp: base.Add(time.Minute), AuthIndex: "ix", AuthID: "x", Provider: "openai", Model: "priced", Source: "sx",
			Tokens: TokenStats{InputTokens: 2000, OutputTokens: 100, TotalTokens: 2100}},
		// Unpriced models, inserted out of alphabetical order.
		Record{ID: "x3", Timestamp: base.Add(2 * time.Minute), AuthIndex: "ix", AuthID: "x", Provider: "openai", Model: "zeta", Failed: true,
			Tokens: TokenStats{InputTokens: 999999, TotalTokens: 999999}},
		Record{ID: "x4", Timestamp: base.Add(-time.Minute), AuthIndex: "ix", AuthID: "x", Provider: "openai", Model: "beta",
			Tokens: TokenStats{InputTokens: 5, TotalTokens: 5}},
		// Account Y (claude): fully priced. 1000*2 + 200*0.5 + 100*1 + 50*8 = 2600 -> 0.0026
		Record{ID: "y1", Timestamp: base, AuthIndex: "iy", AuthID: "y", Provider: "claude", Model: "priced",
			Tokens: TokenStats{InputTokens: 1000, CachedTokens: 200, CacheCreationTokens: 100, OutputTokens: 50, TotalTokens: 1350}},
	)
	got, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Accounts) != 2 || got.Accounts[0].AuthIndex != "ix" || got.Accounts[1].AuthIndex != "iy" {
		t.Fatalf("accounts = %+v", got.Accounts)
	}
	x, y := got.Accounts[0], got.Accounts[1]
	if !near(x.CostUSD, 0.0218) {
		t.Errorf("x cost = %v want 0.0218 (unpriced models excluded)", x.CostUSD)
	}
	if strings.Join(x.UnpricedModels, ",") != "beta,zeta" {
		t.Errorf("x unpriced = %v", x.UnpricedModels)
	}
	// Merging per-model groups keeps whole-account aggregates.
	if x.Requests != 4 || x.Failed != 1 || x.Success != 3 || x.Tokens.TotalTokens != 15500+2100+999999+5 || x.Source != "sx" {
		t.Errorf("x aggregates = %+v", x)
	}
	if !x.FirstRequestAt.Equal(base.Add(-time.Minute)) || !x.LastRequestAt.Equal(base.Add(2*time.Minute)) {
		t.Errorf("x times = %v..%v", x.FirstRequestAt, x.LastRequestAt)
	}
	if !near(y.CostUSD, 0.0026) || y.UnpricedModels == nil || len(y.UnpricedModels) != 0 {
		t.Errorf("y = %+v", y)
	}
	raw, _ := json.Marshal(y)
	if !strings.Contains(string(raw), `"unpriced_models":[]`) || !strings.Contains(string(raw), `"cost_usd":0.0026`) {
		t.Errorf("y json = %s", raw)
	}
	if !near(got.Totals.CostUSD, 0.0244) || got.Totals.Accounts != 2 || got.Totals.Requests != 5 {
		t.Errorf("totals = %+v", got.Totals)
	}
}

func TestListRecordsCost(t *testing.T) {
	store := newTestStore(t)
	putPrices(t, store, testPrice)
	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	insertAll(t, store,
		Record{ID: "a", Timestamp: base, Provider: "openai", Model: "priced",
			Tokens: TokenStats{InputTokens: 10000, CachedTokens: 4000, CacheCreationTokens: 1000, OutputTokens: 500}},
		Record{ID: "b", Timestamp: base.Add(-time.Minute), Provider: "claude", Model: "priced",
			Tokens: TokenStats{InputTokens: 1000, CachedTokens: 200, CacheCreationTokens: 100, OutputTokens: 50}},
		Record{ID: "c", Timestamp: base.Add(-2 * time.Minute), Provider: "openai", Model: "unpriced"},
	)
	page, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 3 || page.Records[0].CostUSD == nil || page.Records[1].CostUSD == nil || page.Records[2].CostUSD != nil {
		t.Fatalf("costs = %+v", page.Records)
	}
	if !near(*page.Records[0].CostUSD, 0.017) || !near(*page.Records[1].CostUSD, 0.0026) {
		t.Fatalf("cost values = %v %v", *page.Records[0].CostUSD, *page.Records[1].CostUSD)
	}
	raw, _ := json.Marshal(page)
	var decoded struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if v, ok := decoded.Records[0]["cost_usd"].(float64); !ok || !near(v, 0.017) {
		t.Errorf("record 0 cost_usd = %v", decoded.Records[0]["cost_usd"])
	}
	if v, ok := decoded.Records[2]["cost_usd"]; !ok || v != nil {
		t.Errorf("record 2 cost_usd = %v present=%v, want JSON null", v, ok)
	}
	// Embedded RequestDetail fields stay at the top level.
	if decoded.Records[0]["id"] != "a" {
		t.Errorf("record 0 = %v", decoded.Records[0])
	}
}

// --- handlers ---

func decodeBody[T any](t *testing.T, resp managementResponse) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(resp.Body, &v); err != nil {
		t.Fatalf("decode %q: %v", resp.Body, err)
	}
	return v
}

func callBody(t *testing.T, method, path, body string) managementResponse {
	t.Helper()
	reqRaw, _ := json.Marshal(managementRequest{Method: method, Path: path, Body: []byte(body)})
	out, err := handleManagement(reqRaw)
	if err != nil {
		t.Fatal(err)
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
	hostPrices   = "/v0/management" + managementPricesPath
	resourceBase = "/v0/resource/plugins/usage-statistics/"
)

func TestHandlePrices(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	insertAll(t, store, Record{ID: "1", Timestamp: time.Now(), Model: "seen-model"})

	resp := callManagement(t, "GET", hostPrices, "")
	if resp.StatusCode != 200 {
		t.Fatalf("GET %d %s", resp.StatusCode, resp.Body)
	}
	if got := decodeBody[PricesResponse](t, resp); len(got.Prices) != 0 || len(got.ModelsSeen) != 1 {
		t.Fatalf("initial = %+v", got)
	}

	resp = callBody(t, "PUT", hostPrices, `{"prices":[{"model":" b ","input":1,"output":2,"cache_read":3,"cache_creation":4},{"model":"a"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("PUT %d %s", resp.StatusCode, resp.Body)
	}
	got := decodeBody[PricesResponse](t, resp)
	if len(got.Prices) != 2 || got.Prices[0].Model != "a" || got.Prices[1].Model != "b" || got.Prices[1].CacheCreation != 4 {
		t.Fatalf("PUT result = %+v", got)
	}
	if again := decodeBody[PricesResponse](t, callManagement(t, "GET", hostPrices, "")); len(again.Prices) != 2 {
		t.Fatalf("GET after PUT = %+v", again)
	}

	for name, c := range map[string]struct{ body, errPrefix string }{
		"invalid json":   {`{not json`, "invalid body"},
		"empty body":     {``, "invalid body"},
		"missing prices": {`{}`, "prices required"},
		"null prices":    {`{"prices":null}`, "prices required"},
		"negative":       {`{"prices":[{"model":"a","input":-1}]}`, "invalid prices: "},
		"empty model":    {`{"prices":[{"model":""}]}`, "invalid prices: "},
		"duplicate":      {`{"prices":[{"model":"a"},{"model":"a"}]}`, "invalid prices: "},
		"too big":        {`{"prices":[{"model":"a","output":10001}]}`, "invalid prices: "},
	} {
		r := callBody(t, "PUT", hostPrices, c.body)
		msg := decodeBody[map[string]string](t, r)["error"]
		if r.StatusCode != 400 || !strings.HasPrefix(msg, c.errPrefix) {
			t.Errorf("%s: %d %q", name, r.StatusCode, r.Body)
		}
	}
	// Rejected requests must not have touched the table.
	if still := decodeBody[PricesResponse](t, callManagement(t, "GET", hostPrices, "")); len(still.Prices) != 2 {
		t.Fatalf("rejected PUT changed prices: %+v", still)
	}

	cleared := callBody(t, "PUT", hostPrices, `{"prices":[]}`)
	if cleared.StatusCode != 200 || len(decodeBody[PricesResponse](t, cleared).Prices) != 0 {
		t.Fatalf("clear = %d %s", cleared.StatusCode, cleared.Body)
	}
	if !strings.Contains(string(cleared.Body), `"prices":[]`) {
		t.Fatalf("clear json = %s", cleared.Body)
	}
	for _, m := range []string{"POST", "DELETE", "PATCH"} {
		if r := callManagement(t, m, hostPrices, ""); r.StatusCode != 405 {
			t.Errorf("%s prices = %d", m, r.StatusCode)
		}
	}
}

func TestResourceUI(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	insertAll(t, store, Record{ID: "secret", Timestamp: time.Now(), AuthIndex: "i", Model: "m", APIKey: "super-secret-key"})

	r := callManagement(t, "GET", resourceBase+"ui", "")
	if r.StatusCode != 200 || len(r.Body) == 0 || string(r.Body) != string(accountsPageHTML) {
		t.Fatalf("ui = %d len=%d", r.StatusCode, len(r.Body))
	}
	want := map[string]string{
		"Content-Type":            "text/html",
		"Content-Security-Policy": accountsPageCSP,
		"X-Content-Type-Options":  "nosniff",
		"Cache-Control":           "no-store",
	}
	for h, v := range want {
		got := strings.Join(r.Headers[h], ",")
		if h == "Content-Type" {
			if !strings.HasPrefix(got, v) {
				t.Errorf("%s = %q", h, got)
			}
		} else if got != v {
			t.Errorf("%s = %q want %q", h, got, v)
		}
	}

	notFound := []struct{ method, path string }{
		{"GET", resourceBase + "accounts"},
		{"GET", resourceBase + "records"},
		{"GET", resourceBase + "prices"},
		{"GET", resourceBase + "usage"},
		{"GET", resourceBase},
		{"GET", resourceBase + "ui/"},
		{"GET", resourceBase + "ui/../records"},
		{"GET", resourceBase + "uix"},
		{"POST", resourceBase + "ui"},
		{"PUT", resourceBase + "ui"},
		{"DELETE", resourceBase + "ui"},
	}
	for _, c := range notFound {
		r := callManagement(t, c.method, c.path, "limit=5")
		if r.StatusCode != 404 {
			t.Errorf("%s %s = %d", c.method, c.path, r.StatusCode)
		}
		if strings.Contains(string(r.Body), "secret") || strings.Contains(string(r.Body), "records") || strings.Contains(string(r.Body), "{") {
			t.Errorf("%s %s leaked data: %s", c.method, c.path, r.Body)
		}
	}
}

// lifecycle drives plugin.* methods against a temp data dir and always leaves
// the process-wide state clean.
func lifecycle(t *testing.T) (call func(method string, payload any) []byte) {
	t.Helper()
	closeStore()
	quiesced.Store(false)
	t.Cleanup(func() {
		closeStore()
		quiesced.Store(false)
		retentionDays.Store(0)
	})
	return func(method string, payload any) []byte {
		t.Helper()
		var raw []byte
		if payload != nil {
			raw, _ = json.Marshal(payload)
		}
		out, err := handleMethod(method, raw)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return out
	}
}

func TestQuiesceAndReregister(t *testing.T) {
	call := lifecycle(t)
	dir := t.TempDir()
	cfg := map[string]any{"config_yaml": "data_dir: '" + dir + "'\n"}
	call("plugin.register", cfg)
	if currentStore() == nil || quiesced.Load() {
		t.Fatal("register did not open the store")
	}

	usage := map[string]any{"Provider": "openai", "Model": "gpt-x", "AuthIndex": "ai", "AuthID": "aid",
		"RequestedAt": "2026-05-02T10:00:00Z", "Detail": map[string]any{"InputTokens": 5, "TotalTokens": 5}}
	if out := string(call("usage.handle", usage)); !strings.Contains(out, `"stored":true`) {
		t.Fatalf("usage.handle = %s", out)
	}

	if out := string(call("plugin.quiesce", nil)); !strings.Contains(out, `"ok":true`) {
		t.Fatalf("quiesce = %s", out)
	}
	if !quiesced.Load() || currentStore() != nil {
		t.Fatal("quiesce did not close the store")
	}
	if out := string(call("usage.handle", usage)); !strings.Contains(out, `"ignored":true`) {
		t.Fatalf("usage.handle while quiesced = %s", out)
	}
	for _, c := range []struct{ method, path, body string }{
		{"GET", hostUsage, ""}, {"DELETE", hostUsage, `{"ids":["x"]}`},
		{"GET", hostAccounts, ""}, {"GET", hostRecords, ""},
		{"GET", hostPrices, ""}, {"PUT", hostPrices, `{"prices":[]}`},
		{"POST", hostRecords, ""},
	} {
		if r := callBody(t, c.method, c.path, c.body); r.StatusCode != 503 {
			t.Errorf("%s %s while quiesced = %d %s", c.method, c.path, r.StatusCode, r.Body)
		}
	}
	if r := callManagement(t, "GET", resourceBase+"ui", ""); r.StatusCode != 200 {
		t.Errorf("ui while quiesced = %d", r.StatusCode)
	}

	call("plugin.reconfigure", cfg)
	if quiesced.Load() || currentStore() == nil {
		t.Fatal("reconfigure did not resume service")
	}
	page := decodePage(t, callManagement(t, "GET", hostRecords, ""))
	if len(page.Records) != 1 || page.Records[0].AuthIndex != "ai" {
		t.Fatalf("rows lost across quiesce: %+v", page.Records)
	}
	if out := string(call("usage.handle", usage)); !strings.Contains(out, `"stored":true`) {
		t.Fatalf("usage.handle after resume = %s", out)
	}
	if page := decodePage(t, callManagement(t, "GET", hostRecords, "")); len(page.Records) != 2 {
		t.Fatalf("records after resume = %d", len(page.Records))
	}

	// register (not only reconfigure) also clears quiesce.
	call("plugin.quiesce", nil)
	call("plugin.register", cfg)
	if quiesced.Load() || callManagement(t, "GET", hostAccounts, "").StatusCode != 200 {
		t.Fatal("register after quiesce did not resume")
	}
}

// --- schema ---

func tableColumns(t *testing.T, store *SQLiteStore, table string) string {
	t.Helper()
	rows, err := store.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
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
	return strings.Join(cols, ",")
}

func TestPriceSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	const usageCols = "id,timestamp,api_key,provider,model,alias,source,auth_id,auth_index,auth_type,executor_type," +
		"reasoning_effort,service_tier,latency_ms,ttft_ms,input_tokens,output_tokens,reasoning_tokens," +
		"cached_tokens,cache_read_tokens,cache_creation_tokens,total_tokens,failed,failure_status_code,failure_body"
	if got := tableColumns(t, store, "usage_records"); got != usageCols {
		t.Fatalf("usage_records columns = %s", got)
	}
	if got := tableColumns(t, store, "model_prices"); got != "model,input,output,cache_read,cache_creation,updated_at" {
		t.Fatalf("model_prices columns = %s", got)
	}
	putPrices(t, store, ModelPrice{Model: "keep", Input: 1})
	insertAll(t, store, Record{ID: "r", Timestamp: time.Now(), Model: "keep"})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening an existing db keeps prices and rows.
	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if p, ok := store.priceFor("keep"); !ok || p.Input != 1 {
		t.Fatalf("price after reopen = %+v ok=%v", p, ok)
	}
	// A db written by an older plugin version has no model_prices table.
	if _, err := store.db.Exec(`DROP TABLE model_prices`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen without model_prices: %v", err)
	}
	defer func() { _ = store.Close() }()
	if got := tableColumns(t, store, "model_prices"); got == "" {
		t.Fatal("model_prices not recreated")
	}
	if got := tableColumns(t, store, "usage_records"); got != usageCols {
		t.Fatalf("usage_records columns after reopen = %s", got)
	}
	res, err := store.Prices(context.Background())
	if err != nil || len(res.Prices) != 0 || len(res.ModelsSeen) != 1 {
		t.Fatalf("prices = %+v err=%v", res, err)
	}
}

// --- review follow-ups ---

func TestQuiesceWaitsForInFlightTx(t *testing.T) {
	call := lifecycle(t)
	dir := t.TempDir()
	cfg := map[string]any{"config_yaml": "data_dir: '" + dir + "'\n"}
	call("plugin.register", cfg)
	store := currentStore()

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO usage_records (id, timestamp, auth_index) VALUES ('inflight', ?, 'ai')`, formatTimestamp(time.Now())); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		quiescePlugin()
		close(done)
	}()
	<-started
	// Give the quiescing goroutine every chance to (wrongly) finish.
	for i := 0; i < 2000; i++ {
		runtime.Gosched()
		select {
		case <-done:
			t.Fatal("quiescePlugin returned while a tx was still open")
		default:
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second): // timeout guard only
		t.Fatal("quiescePlugin did not return after the tx finished")
	}
	if currentStore() != nil || !quiesced.Load() {
		t.Fatal("store still open after quiesce")
	}

	call("plugin.reconfigure", cfg)
	page := decodePage(t, callManagement(t, "GET", hostRecords, "auth_index=ai"))
	if len(page.Records) != 1 || page.Records[0].ID != "inflight" {
		t.Fatalf("in-flight row lost: %+v", page.Records)
	}
}

func TestConcurrentReplacePricesCacheMatchesTable(t *testing.T) {
	store := newTestStore(t)
	const writers = 8
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var set []ModelPrice
			for j := 0; j < 3+w; j++ {
				set = append(set, ModelPrice{Model: fmt.Sprintf("w%d-m%d", w, j), Input: float64(w), Output: float64(j), CacheRead: 0.5, CacheCreation: 1.5})
			}
			for round := 0; round < 5; round++ {
				if err := store.ReplacePrices(context.Background(), set); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	rows, err := store.db.Query(`SELECT model, input, output, cache_read, cache_creation, updated_at FROM model_prices ORDER BY model`)
	if err != nil {
		t.Fatal(err)
	}
	var table []ModelPrice
	var stamps []string
	for rows.Next() {
		var p ModelPrice
		var ts string
		if err := rows.Scan(&p.Model, &p.Input, &p.Output, &p.CacheRead, &p.CacheCreation, &ts); err != nil {
			t.Fatal(err)
		}
		table = append(table, p)
		stamps = append(stamps, ts)
	}
	_ = rows.Close()
	cache := store.cachedPrices()
	if len(table) == 0 || len(cache) != len(table) {
		t.Fatalf("cache %d rows, table %d rows", len(cache), len(table))
	}
	for i := range table {
		c := cache[i]
		stamp := formatTimestamp(c.UpdatedAt)
		c.UpdatedAt = time.Time{}
		if c != table[i] || stamp != stamps[i] {
			t.Fatalf("diverged at %d: cache %+v @%s table %+v @%s", i, c, stamp, table[i], stamps[i])
		}
	}
	// The surviving set must be exactly one writer's set, not a mix.
	owner := strings.SplitN(table[0].Model, "-", 2)[0]
	for _, p := range table {
		if !strings.HasPrefix(p.Model, owner+"-") {
			t.Fatalf("mixed sets: %v", table)
		}
	}
}

func TestLoadPricesSkipsBadRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	good := formatTimestamp(time.Now())
	exec := func(q string, args ...any) error { _, err := store.db.Exec(q, args...); return err }
	if err := exec(`INSERT INTO model_prices (model, input, output, cache_read, cache_creation, updated_at) VALUES ('good', 1, 2, 3, 4, ?)`, good); err != nil {
		t.Fatal(err)
	}
	if err := exec(`INSERT INTO model_prices (model, input, output, cache_read, cache_creation, updated_at) VALUES ('bad-ts', 1, 2, 3, 4, 'not a time')`); err != nil {
		t.Fatal(err)
	}
	textInReal := exec(`INSERT INTO model_prices (model, input, output, cache_read, cache_creation, updated_at) VALUES ('bad-real', 'abc', 2, 3, 4, ?)`, good)
	if textInReal != nil {
		t.Logf("SQLite refused text in REAL column (%v); skipping that row", textInReal)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen with bad price rows: %v", err)
	}
	defer func() { _ = store.Close() }()
	cached := store.cachedPrices()
	if len(cached) != 1 || cached[0].Model != "good" || cached[0].Output != 2 {
		t.Fatalf("cache = %+v", cached)
	}
	insertAll(t, store, Record{ID: "still-works", Timestamp: time.Now(), Model: "good"})
	page, err := store.ListRecords(context.Background(), RecordsQuery{Limit: 5})
	if err != nil || len(page.Records) != 1 || page.Records[0].CostUSD == nil {
		t.Fatalf("records = %+v err=%v", page.Records, err)
	}
}

func TestHandlePricesStrictBody(t *testing.T) {
	store := newTestStore(t)
	useStore(t, store)
	for name, body := range map[string]string{
		"unknown price key": `{"prices":[{"model":"m","cacheRead":0.3}]}`,
		"unknown top key":   `{"prices":[],"extra":1}`,
	} {
		r := callBody(t, "PUT", hostPrices, body)
		if r.StatusCode != 400 || decodeBody[map[string]string](t, r)["error"] != "invalid body" {
			t.Errorf("%s: %d %s", name, r.StatusCode, r.Body)
		}
	}

	callBody(t, "PUT", hostPrices, `{"prices":[{"model":"a","input":1}]}`)
	get := callManagement(t, "GET", hostPrices, "")
	// Round trip of the prices array (carries the known updated_at field).
	prices := decodeBody[PricesResponse](t, get).Prices
	raw, _ := json.Marshal(map[string]any{"prices": prices})
	if r := callBody(t, "PUT", hostPrices, string(raw)); r.StatusCode != 200 {
		t.Errorf("prices round trip: %d %s", r.StatusCode, r.Body)
	}
	// The whole GET body also carries the top-level models_seen key, which
	// DisallowUnknownFields rejects. Pinned as current behaviour (reported to the
	// author as a possible gotcha for clients that echo the GET body).
	if r := callBody(t, "PUT", hostPrices, string(get.Body)); r.StatusCode != 400 {
		t.Errorf("full GET body round trip: %d %s", r.StatusCode, r.Body)
	}

	for name, c := range map[string]struct {
		model string
		want  int
	}{"200 runes": {strings.Repeat("模", 200), 200}, "201 runes": {strings.Repeat("模", 201), 400}} {
		body, _ := json.Marshal(map[string]any{"prices": []map[string]any{{"model": c.model}}})
		if r := callBody(t, "PUT", hostPrices, string(body)); r.StatusCode != c.want {
			t.Errorf("%s: %d %s", name, r.StatusCode, r.Body)
		}
	}
}
