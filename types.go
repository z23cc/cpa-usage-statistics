package main

import "time"

// TokenStats holds token accounting for one request, aligned with upstream
// usage.Detail field semantics.
type TokenStats struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	ReasoningTokens     int64 `json:"reasoning_tokens"`
	CachedTokens        int64 `json:"cached_tokens"`
	CacheReadTokens     int64 `json:"cache_read_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_tokens"`
	TotalTokens         int64 `json:"total_tokens"`
}

// Record is one persisted usage row. Column names mirror upstream
// usage.Record / usage.Detail naming to avoid ambiguity.
type Record struct {
	ID                string
	Timestamp         time.Time
	APIKey            string
	Provider          string
	Model             string
	Alias             string
	Source            string
	AuthID            string
	AuthIndex         string
	AuthType          string
	ExecutorType      string
	ReasoningEffort   string
	ServiceTier       string
	LatencyMs         int64
	TTFTMs            int64
	Tokens            TokenStats
	Failed            bool
	FailureStatusCode int
	FailureBody       string
}

// RequestDetail is the outward JSON shape returned by the usage API. It omits
// storage-only grouping fields (api_key/model live in the map keys).
type RequestDetail struct {
	ID                string     `json:"id"`
	Timestamp         time.Time  `json:"timestamp"`
	Provider          string     `json:"provider,omitempty"`
	Model             string     `json:"model,omitempty"`
	Alias             string     `json:"alias,omitempty"`
	Source            string     `json:"source"`
	AuthID            string     `json:"auth_id,omitempty"`
	AuthIndex         string     `json:"auth_index"`
	AuthType          string     `json:"auth_type,omitempty"`
	ExecutorType      string     `json:"executor_type,omitempty"`
	ReasoningEffort   string     `json:"reasoning_effort"`
	ServiceTier       string     `json:"service_tier"`
	LatencyMs         int64      `json:"latency_ms"`
	TTFTMs            int64      `json:"ttft_ms"`
	Tokens            TokenStats `json:"tokens"`
	Failed            bool       `json:"failed"`
	FailureStatusCode int        `json:"failure_status_code,omitempty"`
	FailureBody       string     `json:"failure_body,omitempty"`
}

// APIUsage groups details by grouping key (api_key or provider) then by model.
type APIUsage map[string]map[string][]RequestDetail

// QueryRange bounds a usage query by timestamp.
type QueryRange struct {
	Start *time.Time
	End   *time.Time
}

// DeleteResult reports the outcome of a delete-by-id request.
type DeleteResult struct {
	Deleted int64    `json:"deleted"`
	Missing []string `json:"missing"`
}

// AccountUsage is the per-credential aggregate returned by the accounts API.
// Accounts are keyed by auth_index (stable per credential file) plus auth_id.
type AccountUsage struct {
	AuthIndex      string     `json:"auth_index"`
	AuthID         string     `json:"auth_id"`
	Source         string     `json:"source"`
	Provider       string     `json:"provider"`
	AuthType       string     `json:"auth_type"`
	Requests       int64      `json:"requests"`
	Success        int64      `json:"success"`
	Failed         int64      `json:"failed"`
	Tokens         TokenStats `json:"tokens"`
	FirstRequestAt time.Time  `json:"first_request_at"`
	LastRequestAt  time.Time  `json:"last_request_at"`
	// CostUSD covers only priced models; UnpricedModels lists the rest.
	CostUSD        float64  `json:"cost_usd"`
	UnpricedModels []string `json:"unpriced_models"`
	// Models breaks the account down per model, by total tokens descending.
	Models []ModelUsage `json:"models"`
}

// ModelUsage is one model's share of an account. CostUSD is null when the
// model has no price.
type ModelUsage struct {
	Model    string     `json:"model"`
	Requests int64      `json:"requests"`
	Success  int64      `json:"success"`
	Failed   int64      `json:"failed"`
	Tokens   TokenStats `json:"tokens"`
	CostUSD  *float64   `json:"cost_usd"`
}

// UsageTotals sums every account in an accounts response.
type UsageTotals struct {
	Accounts int        `json:"accounts"`
	Requests int64      `json:"requests"`
	Success  int64      `json:"success"`
	Failed   int64      `json:"failed"`
	Tokens   TokenStats `json:"tokens"`
	CostUSD  float64    `json:"cost_usd"`
}

// AccountsResponse is the body of GET /plugins/usage-statistics/accounts.
type AccountsResponse struct {
	Accounts []AccountUsage `json:"accounts"`
	Totals   UsageTotals    `json:"totals"`
	// DetailsSince is the earliest retained record (null with none). Ranges
	// before it come from the rollups only and start on the hour.
	DetailsSince *time.Time `json:"details_since"`
	// CostsFixed is always true: every cost_usd is the sum of costs fixed when each
	// request was written, so a later price change never changes it. Consumers that
	// book usage can rely on cost growth instead of pricing token growth themselves.
	CostsFixed bool `json:"costs_fixed"`
}

// RecordsQuery selects one page of records, newest first.
type RecordsQuery struct {
	Range     QueryRange
	AuthIndex string
	AuthID    string
	// Model and Failed narrow the records further; empty / nil match all.
	Model  string
	Failed *bool
	Limit  int
	// Cursor is the opaque next_cursor of the previous page; empty for the first page.
	Cursor string
	// Page (1-based) selects numbered paging instead of Cursor and makes the
	// response carry Total; 0 means cursor paging.
	Page int
}

// RecordDetail is one /records row: the stored record plus its cost. CostUSD
// is null when the record's model has no price. /usage keeps RequestDetail so
// its output stays unchanged.
type RecordDetail struct {
	RequestDetail
	CostUSD *float64 `json:"cost_usd"`
}

// RecordsPage is the body of GET /plugins/usage-statistics/records.
// NextCursor is null on the last page. Total counts every matching record and
// is only present for numbered paging.
type RecordsPage struct {
	Records    []RecordDetail `json:"records"`
	NextCursor *string        `json:"next_cursor"`
	Total      *int64         `json:"total,omitempty"`
}

// HealthResponse is the body of GET /plugins/usage-statistics/health. Arrays,
// not maps keyed by file name: the host HTML-escapes string values only.
type HealthResponse struct {
	AutoDisable  bool             `json:"auto_disable"`
	Accounts     []AccountHealth  `json:"accounts"`
	LastFailures []AccountFailure `json:"last_failures"`
}

// AccountHealth is one tracked Muse credential (see health.go). Message is
// Meta's raw error text; it never contains credential material.
type AccountHealth struct {
	AuthID     string    `json:"auth_id"`
	AuthIndex  string    `json:"auth_index"`
	State      string    `json:"state"`
	Reason     string    `json:"reason"`
	HTTPStatus int       `json:"http_status"`
	Message    string    `json:"message"`
	Since      time.Time `json:"since"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// AccountFailure is a credential's most recent failed request.
type AccountFailure struct {
	AuthID     string    `json:"auth_id"`
	AuthIndex  string    `json:"auth_index"`
	StatusCode int       `json:"status_code"`
	Message    string    `json:"message"`
	At         time.Time `json:"at"`
}

// ModelPrice is one model's price in USD per 1M tokens, the panel's unit.
type ModelPrice struct {
	Model         string    `json:"model"`
	Input         float64   `json:"input"`
	Output        float64   `json:"output"`
	CacheRead     float64   `json:"cache_read"`
	CacheCreation float64   `json:"cache_creation"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// PricesResponse is the body of GET and PUT /plugins/usage-statistics/prices.
type PricesResponse struct {
	Prices []ModelPrice `json:"prices"`
	// ModelsSeen lists every model present in usage_records, priced or not.
	ModelsSeen []string `json:"models_seen"`
}
