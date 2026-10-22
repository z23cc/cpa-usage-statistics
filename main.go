package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// accountsPageHTML is the self-contained per-account usage page served as a
// plugin resource and shown by the management panel in an iframe.
//
//go:embed ui/index.html
var accountsPageHTML []byte

const (
	abiVersion uint32 = 1
	pluginID          = "usage-statistics"
	pluginName        = "Usage Statistics"
	// Plugin-owned management route paths. The host mounts them under
	// /v0/management, producing e.g. /v0/management/plugins/usage-statistics/usage.
	managementUsagePath    = "/plugins/" + pluginID + "/usage"
	managementAccountsPath = "/plugins/" + pluginID + "/accounts"
	managementRecordsPath  = "/plugins/" + pluginID + "/records"
	managementPricesPath   = "/plugins/" + pluginID + "/prices"
	// Resource routes are served by the host under resourceBasePath without
	// management authentication, so only the static page may live there.
	resourceBasePath = "/v0/resource/plugins/" + pluginID + "/"
	accountsPagePath = "/ui"
	accountsPageMenu = "账号用量"
	accountsPageCSP  = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'self'"
	// insertTimeout is long because the host drops a record whose usage.handle
	// fails and never retries; its queue is unbounded, so waiting behind a read
	// on the single connection only delays the record.
	insertTimeout = 30 * time.Second
	queryTimeout  = 5 * time.Second

	defaultRecordsLimit = 100
	maxRecordsLimit     = 1000
)

// Overridable at build time via -ldflags "-X main.pluginVersion=...".
var (
	pluginVersion    = "0.3.0-acct.1"
	pluginAuthor     = "Fwindy"
	pluginRepository = "https://github.com/Fwindy/cpa-usage-statistics"
)

// ---- ABI envelope ----------------------------------------------------------

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func main() {}

func okEnvelope(v any) ([]byte, error) {
	result, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{OK: true, Result: result})
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}

// ---- method dispatch -------------------------------------------------------

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := configurePlugin(request); err != nil {
			return nil, err
		}
		return okEnvelope(registerResponse())
	case "plugin.quiesce":
		quiescePlugin()
		return okEnvelope(map[string]any{"quiesced": true})
	case "usage.handle":
		return handleUsage(request)
	case "management.register":
		return okEnvelope(managementRegistration())
	case "management.handle":
		return handleManagement(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// ---- registration metadata -------------------------------------------------

type registerResponsePayload struct {
	SchemaVersion int              `json:"schema_version"`
	Metadata      metadataInfo     `json:"metadata"`
	Capabilities  capabilitiesInfo `json:"capabilities"`
}

type metadataInfo struct {
	Name             string            `json:"Name"`
	Version          string            `json:"Version"`
	Author           string            `json:"Author"`
	GitHubRepository string            `json:"GitHubRepository"`
	Logo             string            `json:"Logo"`
	ConfigFields     []configFieldInfo `json:"ConfigFields"`
}

type configFieldInfo struct {
	Name        string `json:"Name"`
	Type        string `json:"Type"`
	Description string `json:"Description"`
}

type capabilitiesInfo struct {
	UsagePlugin   bool `json:"usage_plugin"`
	ManagementAPI bool `json:"management_api"`
}

func registerResponse() registerResponsePayload {
	return registerResponsePayload{
		SchemaVersion: 1,
		Metadata: metadataInfo{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           pluginAuthor,
			GitHubRepository: pluginRepository,
			ConfigFields: []configFieldInfo{
				{Name: "data_dir", Type: "string", Description: "Directory for usage.db. Defaults to ~/.cli-proxy-api/plugins/usage-statistics."},
				{Name: "retention_days", Type: "integer", Description: "Delete usage records older than this many days. 0 disables cleanup."},
			},
		},
		Capabilities: capabilitiesInfo{UsagePlugin: true, ManagementAPI: true},
	}
}

type managementRegistrationPayload struct {
	Routes    []managementRoute `json:"routes"`
	Resources []resourceRoute   `json:"resources,omitempty"`
}

type managementRoute struct {
	Method      string `json:"Method"`
	Path        string `json:"Path"`
	Description string `json:"Description"`
}

// resourceRoute is a browser-navigable page under resourceBasePath; Menu makes
// the management panel list it in the sidebar.
type resourceRoute struct {
	Path        string `json:"Path"`
	Menu        string `json:"Menu"`
	Description string `json:"Description"`
}

func managementRegistration() managementRegistrationPayload {
	return managementRegistrationPayload{
		Routes: []managementRoute{
			{Method: "GET", Path: managementUsagePath, Description: "Query persisted usage grouped by api key and model."},
			{Method: "DELETE", Path: managementUsagePath, Description: "Delete persisted usage records by id."},
			{Method: "GET", Path: managementAccountsPath, Description: "Aggregate persisted usage and cost per credential."},
			{Method: "GET", Path: managementRecordsPath, Description: "Page persisted usage records newest first, optionally for one credential."},
			{Method: "GET", Path: managementPricesPath, Description: "List model prices (USD per 1M tokens) and the models seen."},
			{Method: "PUT", Path: managementPricesPath, Description: "Replace all model prices."},
		},
		Resources: []resourceRoute{
			{Path: accountsPagePath, Menu: accountsPageMenu, Description: "Per-account usage, cost and request details."},
		},
	}
}

// ---- usage ingestion -------------------------------------------------------

type usageRecord struct {
	Provider        string       `json:"Provider"`
	ExecutorType    string       `json:"ExecutorType"`
	Model           string       `json:"Model"`
	Alias           string       `json:"Alias"`
	APIKey          string       `json:"APIKey"`
	AuthID          string       `json:"AuthID"`
	AuthIndex       string       `json:"AuthIndex"`
	AuthType        string       `json:"AuthType"`
	Source          string       `json:"Source"`
	ReasoningEffort string       `json:"ReasoningEffort"`
	ServiceTier     string       `json:"ServiceTier"`
	RequestedAt     time.Time    `json:"RequestedAt"`
	Latency         int64        `json:"Latency"`
	TTFT            int64        `json:"TTFT"`
	Failed          bool         `json:"Failed"`
	Failure         usageFailure `json:"Failure"`
	Detail          usageDetail  `json:"Detail"`
}

type usageFailure struct {
	StatusCode int    `json:"StatusCode"`
	Body       string `json:"Body"`
}

type usageDetail struct {
	InputTokens         int64 `json:"InputTokens"`
	OutputTokens        int64 `json:"OutputTokens"`
	ReasoningTokens     int64 `json:"ReasoningTokens"`
	CachedTokens        int64 `json:"CachedTokens"`
	CacheReadTokens     int64 `json:"CacheReadTokens"`
	CacheCreationTokens int64 `json:"CacheCreationTokens"`
	TotalTokens         int64 `json:"TotalTokens"`
}

func handleUsage(request []byte) ([]byte, error) {
	if len(request) == 0 {
		return okEnvelope(map[string]any{"ignored": true})
	}
	var rec usageRecord
	if err := json.Unmarshal(request, &rec); err != nil {
		return okEnvelope(map[string]any{"ignored": true})
	}
	store := currentStore()
	if store == nil {
		return okEnvelope(map[string]any{"ignored": true})
	}
	ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
	defer cancel()
	if err := store.Insert(ctx, toRecord(rec)); err != nil {
		return nil, err
	}
	maybeCleanup()
	return okEnvelope(map[string]any{"stored": true})
}

func toRecord(rec usageRecord) Record {
	ts := rec.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	return Record{
		ID:              uuid.NewString(),
		Timestamp:       ts.UTC(),
		APIKey:          strings.TrimSpace(rec.APIKey),
		Provider:        strings.TrimSpace(rec.Provider),
		Model:           normalizeModel(rec.Model),
		Alias:           strings.TrimSpace(rec.Alias),
		Source:          strings.TrimSpace(rec.Source),
		AuthID:          strings.TrimSpace(rec.AuthID),
		AuthIndex:       strings.TrimSpace(rec.AuthIndex),
		AuthType:        strings.TrimSpace(rec.AuthType),
		ExecutorType:    strings.TrimSpace(rec.ExecutorType),
		ReasoningEffort: strings.TrimSpace(rec.ReasoningEffort),
		ServiceTier:     strings.TrimSpace(rec.ServiceTier),
		LatencyMs:       nsToMs(rec.Latency),
		TTFTMs:          nsToMs(rec.TTFT),
		Tokens: TokenStats{
			InputTokens:         rec.Detail.InputTokens,
			OutputTokens:        rec.Detail.OutputTokens,
			ReasoningTokens:     rec.Detail.ReasoningTokens,
			CachedTokens:        rec.Detail.CachedTokens,
			CacheReadTokens:     rec.Detail.CacheReadTokens,
			CacheCreationTokens: rec.Detail.CacheCreationTokens,
			TotalTokens:         rec.Detail.TotalTokens,
		},
		Failed:            rec.Failed || rec.Failure.StatusCode >= 400,
		FailureStatusCode: rec.Failure.StatusCode,
		FailureBody:       strings.TrimSpace(rec.Failure.Body),
	}
}

func nsToMs(ns int64) int64 {
	if ns <= 0 {
		return 0
	}
	return ns / int64(time.Millisecond)
}

// ---- management handling ---------------------------------------------------

type managementRequest struct {
	Method  string              `json:"Method"`
	Path    string              `json:"Path"`
	Headers map[string][]string `json:"Headers"`
	Query   map[string][]string `json:"Query"`
	Body    []byte              `json:"Body"`
}

type managementResponse struct {
	StatusCode int                 `json:"StatusCode"`
	Headers    map[string][]string `json:"Headers"`
	Body       []byte              `json:"Body"`
}

type deleteUsageRequest struct {
	IDs []string `json:"ids"`
}

func handleManagement(request []byte) ([]byte, error) {
	var req managementRequest
	if len(request) > 0 {
		if err := json.Unmarshal(request, &req); err != nil {
			return nil, err
		}
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	// The host passes the full request path. Resource requests skip management
	// auth, so they are answered first and can never reach a data handler.
	if strings.HasPrefix(req.Path, resourceBasePath) {
		return resourceGet(method, req.Path)
	}
	if quiesced.Load() {
		return okEnvelope(jsonManagementResponse(503, map[string]string{"error": "usage store is being replaced"}))
	}
	switch {
	case strings.HasSuffix(req.Path, managementPricesPath):
		switch method {
		case "GET":
			return pricesGet()
		case "PUT":
			return pricesPut(req)
		default:
			return okEnvelope(jsonManagementResponse(405, map[string]string{"error": "method not allowed"}))
		}
	case strings.HasSuffix(req.Path, managementAccountsPath):
		if method != "GET" {
			return okEnvelope(jsonManagementResponse(405, map[string]string{"error": "method not allowed"}))
		}
		return accountsGet(req)
	case strings.HasSuffix(req.Path, managementRecordsPath):
		if method != "GET" {
			return okEnvelope(jsonManagementResponse(405, map[string]string{"error": "method not allowed"}))
		}
		return recordsGet(req)
	}
	switch method {
	case "GET":
		return usageGet(req)
	case "DELETE":
		return usageDelete(req)
	default:
		return okEnvelope(jsonManagementResponse(405, map[string]string{"error": "method not allowed"}))
	}
}

func accountsGet(req managementRequest) ([]byte, error) {
	rng, errResp := parseUsageRange(req.Query)
	if errResp != nil {
		return okEnvelope(*errResp)
	}
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(200, AccountsResponse{Accounts: []AccountUsage{}}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	result, err := store.AccountSummary(ctx, rng)
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query account usage"}))
	}
	return okEnvelope(jsonManagementResponse(200, result))
}

func recordsGet(req managementRequest) ([]byte, error) {
	rng, errResp := parseUsageRange(req.Query)
	if errResp != nil {
		return okEnvelope(*errResp)
	}
	limit := defaultRecordsLimit
	if raw := strings.TrimSpace(firstValue(req.Query, "limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "invalid limit"}))
		}
		limit = min(parsed, maxRecordsLimit)
	}
	q := RecordsQuery{
		Range:     rng,
		AuthIndex: strings.TrimSpace(firstValue(req.Query, "auth_index")),
		AuthID:    strings.TrimSpace(firstValue(req.Query, "auth_id")),
		Limit:     limit,
		Cursor:    strings.TrimSpace(firstValue(req.Query, "cursor")),
	}
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(200, RecordsPage{Records: []RecordDetail{}}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	page, err := store.ListRecords(ctx, q)
	if errors.Is(err, errInvalidCursor) {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "invalid cursor"}))
	}
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query usage records"}))
	}
	return okEnvelope(jsonManagementResponse(200, page))
}

// resourceGet serves the static accounts page and nothing else.
func resourceGet(method, path string) ([]byte, error) {
	if method != "GET" || path != resourceBasePath+strings.TrimPrefix(accountsPagePath, "/") {
		return okEnvelope(managementResponse{
			StatusCode: 404,
			Headers:    map[string][]string{"Content-Type": {"text/plain; charset=utf-8"}},
			Body:       []byte("not found"),
		})
	}
	return okEnvelope(managementResponse{
		StatusCode: 200,
		Headers: map[string][]string{
			"Content-Type":            {"text/html; charset=utf-8"},
			"Content-Security-Policy": {accountsPageCSP},
			"X-Content-Type-Options":  {"nosniff"},
			"Referrer-Policy":         {"no-referrer"},
			// A new plugin build ships a new page; never serve a stale copy.
			"Cache-Control": {"no-store"},
		},
		Body: accountsPageHTML,
	})
}

func pricesGet() ([]byte, error) {
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(200, PricesResponse{Prices: []ModelPrice{}, ModelsSeen: []string{}}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	result, err := store.Prices(ctx)
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query prices"}))
	}
	return okEnvelope(jsonManagementResponse(200, result))
}

// pricesPut replaces the whole price table. An explicit empty list clears it;
// a body without "prices" is rejected so a malformed request cannot wipe it.
func pricesPut(req managementRequest) ([]byte, error) {
	var body struct {
		Prices *[]ModelPrice `json:"prices"`
	}
	// Reject unknown keys, e.g. the panel's cacheRead spelling, instead of
	// silently saving a zero price.
	decoder := json.NewDecoder(bytes.NewReader(req.Body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "invalid body"}))
	}
	if body.Prices == nil {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "prices required"}))
	}
	prices, err := validatePrices(*body.Prices)
	if err != nil {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "invalid prices: " + err.Error()}))
	}
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(503, map[string]string{"error": "usage store unavailable"}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	if err := store.ReplacePrices(ctx, prices); err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to save prices"}))
	}
	// The save is committed; a failing models_seen lookup must not turn it into an error.
	result, err := store.Prices(ctx)
	if err != nil {
		result = PricesResponse{Prices: store.cachedPrices(), ModelsSeen: []string{}}
	}
	return okEnvelope(jsonManagementResponse(200, result))
}

func usageGet(req managementRequest) ([]byte, error) {
	rng, errResp := parseUsageRange(req.Query)
	if errResp != nil {
		return okEnvelope(*errResp)
	}
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(200, APIUsage{}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	result, err := store.Query(ctx, rng)
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to query usage"}))
	}
	return okEnvelope(jsonManagementResponse(200, result))
}

func usageDelete(req managementRequest) ([]byte, error) {
	store := currentStore()
	if store == nil {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "usage store unavailable"}))
	}
	var body deleteUsageRequest
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "invalid body"}))
		}
	}
	ids := make([]string, 0, len(body.IDs))
	seen := make(map[string]struct{}, len(body.IDs))
	for _, id := range body.IDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		ids = append(ids, trimmed)
	}
	if len(ids) == 0 {
		return okEnvelope(jsonManagementResponse(400, map[string]string{"error": "ids required"}))
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	result, err := store.Delete(ctx, ids)
	if err != nil {
		return okEnvelope(jsonManagementResponse(500, map[string]string{"error": "failed to delete usage records"}))
	}
	return okEnvelope(jsonManagementResponse(200, result))
}

func parseUsageRange(query map[string][]string) (QueryRange, *managementResponse) {
	var rng QueryRange
	if raw := strings.TrimSpace(firstValue(query, "start")); raw != "" {
		start, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			resp := jsonManagementResponse(400, map[string]string{"error": "invalid start"})
			return rng, &resp
		}
		start = start.UTC()
		rng.Start = &start
	}
	if raw := strings.TrimSpace(firstValue(query, "end")); raw != "" {
		end, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			resp := jsonManagementResponse(400, map[string]string{"error": "invalid end"})
			return rng, &resp
		}
		end = end.UTC()
		rng.End = &end
	}
	return rng, nil
}

func firstValue(values map[string][]string, key string) string {
	if len(values[key]) > 0 {
		return values[key][0]
	}
	return ""
}

func jsonManagementResponse(status int, payload any) managementResponse {
	body, err := json.Marshal(payload)
	if err != nil {
		body = []byte(`{"error":"failed to encode response"}`)
		status = 500
	}
	return managementResponse{
		StatusCode: status,
		Headers:    map[string][]string{"Content-Type": {"application/json; charset=utf-8"}},
		Body:       body,
	}
}

// ---- store lifecycle -------------------------------------------------------

var (
	storeMu       sync.RWMutex
	globalStore   *SQLiteStore
	currentDBPath string

	retentionDays atomic.Int64

	cleanupMu   sync.Mutex
	lastCleanup time.Time

	// quiesced is set by plugin.quiesce, which the host sends to the old
	// instance right before a hot reload loads the new one. Closing the db then
	// keeps two SQLite copies in one process from writing usage.db together.
	quiesced atomic.Bool
)

// lifecycleMu serialises quiesce with register/reconfigure, so a host rollback
// that re-registers this instance cannot interleave with a slow quiesce and
// leave the plugin active without a store.
var lifecycleMu sync.Mutex

// quiescePlugin closes the store and returns only after in-flight database
// work has finished (see SQLiteStore.Close). Usage records arriving afterwards
// are ignored until the next register.
func quiescePlugin() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	quiesced.Store(true)
	closeStore()
}

func currentStore() *SQLiteStore {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return globalStore
}

// closeStore detaches the store first so new callers see nil at once, then
// waits for in-flight work in Close without holding storeMu.
func closeStore() {
	storeMu.Lock()
	store := globalStore
	globalStore = nil
	currentDBPath = ""
	storeMu.Unlock()
	if store != nil {
		_ = store.Close()
	}
}

func ensureStore(cfg pluginConfig) error {
	path, err := resolveDBPath(cfg)
	if err != nil {
		return err
	}
	storeMu.Lock()
	defer storeMu.Unlock()
	if globalStore != nil && currentDBPath == path {
		return nil
	}
	store, err := NewSQLiteStore(path)
	if err != nil {
		return err
	}
	if globalStore != nil {
		_ = globalStore.Close()
	}
	globalStore = store
	currentDBPath = path
	return nil
}

func maybeCleanup() {
	days := retentionDays.Load()
	if days <= 0 {
		return
	}
	cleanupMu.Lock()
	now := time.Now()
	if !lastCleanup.IsZero() && now.Sub(lastCleanup) < time.Hour {
		cleanupMu.Unlock()
		return
	}
	lastCleanup = now
	cleanupMu.Unlock()

	store := currentStore()
	if store == nil {
		return
	}
	cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
	defer cancel()
	_, _ = store.DeleteBefore(ctx, cutoff)
}

// ---- config ----------------------------------------------------------------

type lifecycleRequest struct {
	ConfigYAML json.RawMessage `json:"config_yaml"`
}

func configurePlugin(request []byte) error {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	var req lifecycleRequest
	if len(request) > 0 {
		if err := json.Unmarshal(request, &req); err != nil {
			return err
		}
	}
	raw, err := lifecycleConfigYAML(req.ConfigYAML)
	if err != nil {
		return err
	}
	cfg := parseConfig(raw)
	retentionDays.Store(int64(cfg.RetentionDays))
	if err := ensureStore(cfg); err != nil {
		return err
	}
	// A register after quiesce (e.g. the host rolling back a failed reload)
	// has just reopened the store, so resume normal service.
	quiesced.Store(false)
	if cfg.RetentionDays > 0 {
		cleanupMu.Lock()
		lastCleanup = time.Time{}
		cleanupMu.Unlock()
		maybeCleanup()
	}
	return nil
}

// lifecycleConfigYAML accepts the host's config_yaml which may arrive as a
// base64 string, a plain YAML string, or a byte array.
func lifecycleConfigYAML(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		if decoded, errDecode := base64.StdEncoding.DecodeString(text); errDecode == nil && strings.Contains(string(decoded), ":") {
			return decoded, nil
		}
		return []byte(text), nil
	}
	var bytes []byte
	if err := json.Unmarshal(raw, &bytes); err == nil {
		return bytes, nil
	}
	return nil, errors.New("config_yaml must be a string or byte array")
}
