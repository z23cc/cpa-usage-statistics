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
}

// UsageTotals sums every account in an accounts response.
type UsageTotals struct {
	Accounts int        `json:"accounts"`
	Requests int64      `json:"requests"`
	Success  int64      `json:"success"`
	Failed   int64      `json:"failed"`
	Tokens   TokenStats `json:"tokens"`
}

// AccountsResponse is the body of GET /plugins/usage-statistics/accounts.
type AccountsResponse struct {
	Accounts []AccountUsage `json:"accounts"`
	Totals   UsageTotals    `json:"totals"`
}

// RecordsQuery selects one page of records, newest first.
type RecordsQuery struct {
	Range     QueryRange
	AuthIndex string
	AuthID    string
	Limit     int
	// Cursor is the opaque next_cursor of the previous page; empty for the first page.
	Cursor string
}

// RecordsPage is the body of GET /plugins/usage-statistics/records.
// NextCursor is null on the last page.
type RecordsPage struct {
	Records    []RequestDetail `json:"records"`
	NextCursor *string         `json:"next_cursor"`
}
