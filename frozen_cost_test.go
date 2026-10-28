package main

import (
	"context"
	"math"
	"path/filepath"
	"testing"
)

func frozenRecord(id string, minute int) Record {
	return Record{
		ID: id, Timestamp: utc(10, 12, minute, 0), Provider: "meta", Model: "muse-x",
		AuthID: "acct.json", AuthIndex: "1",
		Tokens: TokenStats{InputTokens: 1_000_000, CachedTokens: 400_000, OutputTokens: 100_000},
	}
}

func frozenAccountCost(t *testing.T, store *SQLiteStore) (float64, []string) {
	t.Helper()
	summary, err := store.AccountSummary(context.Background(), QueryRange{})
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Accounts) != 1 {
		t.Fatalf("accounts = %d", len(summary.Accounts))
	}
	return summary.Accounts[0].CostUSD, summary.Accounts[0].UnpricedModels
}

func TestFrozenCost_PriceChangeOnlyAffectsLaterRecords(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	putPrices(t, store, ModelPrice{Model: "muse-x", Input: 5, Output: 25, CacheRead: 0.2})
	if err := store.Insert(ctx, frozenRecord("a", 0)); err != nil {
		t.Fatal(err)
	}
	// 600k uncached * 5 + 400k cached * 0.2 + 100k output * 25, per 1M.
	first := 3.0 + 0.08 + 2.5
	if got, _ := frozenAccountCost(t, store); math.Abs(got-first) > 1e-9 {
		t.Fatalf("cost after first record = %v, want %v", got, first)
	}

	putPrices(t, store, ModelPrice{Model: "muse-x", Input: 5, Output: 5, CacheRead: 0.01})
	if got, _ := frozenAccountCost(t, store); math.Abs(got-first) > 1e-9 {
		t.Fatalf("a price change repriced history: cost = %v, want %v", got, first)
	}
	if err := store.Insert(ctx, frozenRecord("b", 1)); err != nil {
		t.Fatal(err)
	}
	second := 3.0 + 0.004 + 0.5
	if got, _ := frozenAccountCost(t, store); math.Abs(got-(first+second)) > 1e-9 {
		t.Fatalf("cost after second record = %v, want %v", got, first+second)
	}
	page, err := store.ListRecords(ctx, RecordsQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	costs := map[string]float64{}
	for _, r := range page.Records {
		if r.CostUSD == nil {
			t.Fatalf("record %s has no stored cost", r.ID)
		}
		costs[r.ID] = *r.CostUSD
	}
	if math.Abs(costs["a"]-first) > 1e-6 || math.Abs(costs["b"]-second) > 1e-6 {
		t.Fatalf("record costs = %v", costs)
	}

	// Deleting a record takes its own stored cost out of the rollups.
	if _, err := store.Delete(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := frozenAccountCost(t, store); math.Abs(got-second) > 1e-9 {
		t.Fatalf("cost after deleting a = %v, want %v", got, second)
	}
}

func TestFrozenCost_UnpricedRecordsStayUnpriced(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.Insert(ctx, frozenRecord("a", 0)); err != nil {
		t.Fatal(err)
	}
	putPrices(t, store, ModelPrice{Model: "muse-x", Input: 5, Output: 25, CacheRead: 0.2})
	cost, unpriced := frozenAccountCost(t, store)
	if cost != 0 || len(unpriced) != 1 || unpriced[0] != "muse-x" {
		t.Fatalf("a record written before its price was priced later: cost=%v unpriced=%v", cost, unpriced)
	}
}

func TestFrozenCost_MigrationFromVersion1KeepsLegacyRecordsUnpriced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.db")
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	putPrices(t, store, ModelPrice{Model: "muse-x", Input: 5, Output: 25, CacheRead: 0.2})
	if err := store.Insert(ctx, frozenRecord("legacy", 0)); err != nil {
		t.Fatal(err)
	}
	// Turn the database back into a version-1 one: no stored costs, no rollup cost columns.
	for _, statement := range []string{
		`DROP TRIGGER usage_records_rollup_ai`,
		`DROP TRIGGER usage_records_rollup_ad`,
		`UPDATE usage_records SET cost_usd = NULL`,
		`ALTER TABLE usage_hourly DROP COLUMN cost_usd`,
		`ALTER TABLE usage_hourly DROP COLUMN unpriced_requests`,
		`ALTER TABLE usage_daily DROP COLUMN cost_usd`,
		`ALTER TABLE usage_daily DROP COLUMN unpriced_requests`,
		`PRAGMA user_version = 1`,
	} {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if _, err := store.db.ExecContext(ctx, rollupInsertTriggerV1ForTest()); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	store, err = NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen after migration: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var version int
	if err := store.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version != rollupSchemaVersion {
		t.Fatalf("user_version = %d err=%v", version, err)
	}
	cost, unpriced := frozenAccountCost(t, store)
	if cost != 0 || len(unpriced) != 1 {
		t.Fatalf("legacy record was priced by the migration: cost=%v unpriced=%v", cost, unpriced)
	}
	if err := store.Insert(ctx, frozenRecord("new", 1)); err != nil {
		t.Fatal(err)
	}
	if cost, _ := frozenAccountCost(t, store); math.Abs(cost-(3.0+0.08+2.5)) > 1e-9 {
		t.Fatalf("new record after migration: cost = %v", cost)
	}
}

// rollupInsertTriggerV1ForTest is a version-1 style insert trigger (tokens only), so the
// migration test starts from triggers that do not know the cost columns.
func rollupInsertTriggerV1ForTest() string {
	return "CREATE TRIGGER usage_records_rollup_ai AFTER INSERT ON usage_records BEGIN SELECT 1; END"
}
