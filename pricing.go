package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	tokensPerPriceUnit = 1_000_000
	maxPriceEntries    = 500
	maxPriceModelLen   = 200
	maxPricePerMTok    = 10000
)

// pureInputSQL is the SQL form of pureInputTokens, used when aggregating.
const pureInputSQL = `CASE WHEN lower(provider) LIKE '%claude%' OR lower(provider) LIKE '%anthropic%'
       THEN input_tokens ELSE MAX(input_tokens - cached_tokens - cache_creation_tokens, 0) END`

// isAnthropicProvider mirrors the panel: Claude/Anthropic input_tokens exclude
// cache tokens, every other provider includes them.
func isAnthropicProvider(provider string) bool {
	name := strings.ToLower(provider)
	return strings.Contains(name, "claude") || strings.Contains(name, "anthropic")
}

// pureInputTokens is the uncached input of one record, as the panel counts it.
func pureInputTokens(provider string, tokens TokenStats) int64 {
	if isAnthropicProvider(provider) {
		return tokens.InputTokens
	}
	return max(tokens.InputTokens-tokens.CachedTokens-tokens.CacheCreationTokens, 0)
}

// costUSD mirrors the panel's calculateCost without context tiers or service
// tier multipliers. output_tokens already include reasoning tokens.
func costUSD(price ModelPrice, pureInput, cacheRead, cacheCreation, output int64) float64 {
	return (float64(pureInput)*price.Input +
		float64(cacheRead)*price.CacheRead +
		float64(cacheCreation)*price.CacheCreation +
		float64(output)*price.Output) / tokensPerPriceUnit
}

// roundUSD keeps responses readable; 1e-6 USD is far below any displayed digit.
func roundUSD(value float64) float64 {
	return math.Round(value*1e6) / 1e6
}

// initPriceSchema creates the price table. It is a separate table, so older
// plugin versions ignore it and can still open the db after a rollback.
func (s *SQLiteStore) initPriceSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS model_prices (
	model TEXT PRIMARY KEY,
	input REAL NOT NULL DEFAULT 0 CHECK (input >= 0),
	output REAL NOT NULL DEFAULT 0 CHECK (output >= 0),
	cache_read REAL NOT NULL DEFAULT 0 CHECK (cache_read >= 0),
	cache_creation REAL NOT NULL DEFAULT 0 CHECK (cache_creation >= 0),
	updated_at TEXT NOT NULL
)`)
	if err != nil {
		return fmt.Errorf("usage sqlite init price schema: %w", err)
	}
	return nil
}

// loadPrices refreshes the in-memory price cache from the table. A row that
// does not scan (e.g. edited by hand) is skipped: a bad price must never stop
// the plugin from recording usage.
func (s *SQLiteStore) loadPrices(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT model, input, output, cache_read, cache_creation, updated_at FROM model_prices ORDER BY model`)
	if err != nil {
		return fmt.Errorf("usage sqlite load prices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	prices := make(map[string]ModelPrice)
	for rows.Next() {
		var price ModelPrice
		var updatedText string
		if err := rows.Scan(&price.Model, &price.Input, &price.Output, &price.CacheRead, &price.CacheCreation, &updatedText); err != nil {
			continue
		}
		if price.UpdatedAt, err = parseStoredTimestamp(updatedText); err != nil {
			continue
		}
		prices[price.Model] = price
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("usage sqlite load prices rows: %w", err)
	}
	s.setPrices(prices)
	return nil
}

func (s *SQLiteStore) setPrices(prices map[string]ModelPrice) {
	s.pricesMu.Lock()
	s.prices = prices
	s.pricesMu.Unlock()
}

// cachedPrices returns the cached prices sorted by model.
func (s *SQLiteStore) cachedPrices() []ModelPrice {
	s.pricesMu.RLock()
	out := make([]ModelPrice, 0, len(s.prices))
	for _, price := range s.prices {
		out = append(out, price)
	}
	s.pricesMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// priceFor returns the cached price of model.
func (s *SQLiteStore) priceFor(model string) (ModelPrice, bool) {
	s.pricesMu.RLock()
	defer s.pricesMu.RUnlock()
	price, ok := s.prices[model]
	return price, ok
}

// Prices returns every stored price plus the models seen in usage records.
func (s *SQLiteStore) Prices(ctx context.Context) (PricesResponse, error) {
	result := PricesResponse{Prices: []ModelPrice{}, ModelsSeen: []string{}}
	if s == nil || s.db == nil {
		return result, nil
	}
	result.Prices = s.cachedPrices()

	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT model FROM usage_records ORDER BY model`)
	if err != nil {
		return result, fmt.Errorf("usage sqlite models seen: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var model string
		if err := rows.Scan(&model); err != nil {
			return result, fmt.Errorf("usage sqlite models seen scan: %w", err)
		}
		result.ModelsSeen = append(result.ModelsSeen, model)
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("usage sqlite models seen rows: %w", err)
	}
	return result, nil
}

// ReplacePrices swaps the whole price table in one transaction and then sets
// the cache from the same validated slice, so the cache matches what was
// committed even when two saves race. prices must already be validated. An
// error means nothing was committed.
func (s *SQLiteStore) ReplacePrices(ctx context.Context, prices []ModelPrice) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage sqlite store is nil")
	}
	s.replaceMu.Lock()
	defer s.replaceMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("usage sqlite replace prices begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM model_prices`); err != nil {
		return fmt.Errorf("usage sqlite replace prices clear: %w", err)
	}
	now := time.Now().UTC()
	nowText := formatTimestamp(now)
	cache := make(map[string]ModelPrice, len(prices))
	for _, price := range prices {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO model_prices (model, input, output, cache_read, cache_creation, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
			price.Model, price.Input, price.Output, price.CacheRead, price.CacheCreation, nowText); err != nil {
			return fmt.Errorf("usage sqlite replace prices insert %s: %w", price.Model, err)
		}
		price.UpdatedAt = now
		cache[price.Model] = price
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("usage sqlite replace prices commit: %w", err)
	}
	s.setPrices(cache)
	return nil
}

// validatePrices checks a PUT body entry by entry and returns trimmed copies.
func validatePrices(prices []ModelPrice) ([]ModelPrice, error) {
	if len(prices) > maxPriceEntries {
		return nil, fmt.Errorf("at most %d prices", maxPriceEntries)
	}
	seen := make(map[string]struct{}, len(prices))
	out := make([]ModelPrice, 0, len(prices))
	for i, price := range prices {
		price.Model = strings.TrimSpace(price.Model)
		if price.Model == "" || utf8.RuneCountInString(price.Model) > maxPriceModelLen {
			return nil, fmt.Errorf("entry %d: model must be 1-%d characters", i, maxPriceModelLen)
		}
		if _, dup := seen[price.Model]; dup {
			return nil, fmt.Errorf("entry %d: duplicate model %q", i, price.Model)
		}
		seen[price.Model] = struct{}{}
		for _, field := range []struct {
			name  string
			value float64
		}{{"input", price.Input}, {"output", price.Output}, {"cache_read", price.CacheRead}, {"cache_creation", price.CacheCreation}} {
			if math.IsNaN(field.value) || field.value < 0 || field.value > maxPricePerMTok {
				return nil, fmt.Errorf("entry %d: %s must be between 0 and %d", i, field.name, maxPricePerMTok)
			}
		}
		price.UpdatedAt = time.Time{}
		out = append(out, price)
	}
	return out, nil
}
