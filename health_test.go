package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- fake host ------------------------------------------------------------------

type fakeAuth struct {
	name        string
	json        string
	disabled    bool
	lastRefresh time.Time
	status      string
	unavailable bool
}

type logLine struct{ level, message string }

type fakeHost struct {
	mu          sync.Mutex
	auths       map[string]*fakeAuth // keyed by auth_index
	runtimeErr  error
	httpResp    hostHTTPResponse
	httpErr     error
	onHTTP      func() // runs inside httpDo, before the response is returned
	requests    []hostHTTPRequest
	saves       []string // names of saved files
	resets      []string // auth indexes whose cooldown was reset
	logs        []logLine
	runtimeHits int
}

func newFakeHost() *fakeHost { return &fakeHost{auths: map[string]*fakeAuth{}} }

func (h *fakeHost) add(idx, name, doc string) *fakeAuth {
	a := &fakeAuth{name: name, json: doc}
	h.mu.Lock()
	h.auths[idx] = a
	h.mu.Unlock()
	return a
}

func (h *fakeHost) authRuntime(idx string) (hostAuthRuntime, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.runtimeHits++
	if h.runtimeErr != nil {
		return hostAuthRuntime{}, h.runtimeErr
	}
	a, ok := h.auths[idx]
	if !ok {
		return hostAuthRuntime{}, errHostAuthNotFound
	}
	return hostAuthRuntime{AuthIndex: idx, Name: a.name, Disabled: a.disabled, LastRefresh: a.lastRefresh,
		Status: a.status, Unavailable: a.unavailable}, nil
}

func (h *fakeHost) authGet(idx string) (hostAuthFile, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	a, ok := h.auths[idx]
	if !ok {
		return hostAuthFile{}, errHostAuthNotFound
	}
	return hostAuthFile{Name: a.name, JSON: json.RawMessage(a.json)}, nil
}

func (h *fakeHost) authSave(name string, data []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, a := range h.auths {
		if a.name == name {
			a.json = string(data)
			var doc map[string]any
			_ = json.Unmarshal(data, &doc)
			a.disabled, _ = doc["disabled"].(bool)
			h.saves = append(h.saves, name)
			return nil
		}
	}
	return fmt.Errorf("save %s: unknown file", name)
}

func (h *fakeHost) resetCooldown(idx string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resets = append(h.resets, idx)
	return nil
}

func (h *fakeHost) httpDo(_ context.Context, req hostHTTPRequest) (hostHTTPResponse, error) {
	h.mu.Lock()
	h.requests = append(h.requests, req)
	hook := h.onHTTP
	resp, err := h.httpResp, h.httpErr
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return resp, err
}

func (h *fakeHost) log(level, message string, _ map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, logLine{level, message})
}

func (h *fakeHost) setJSON(idx, doc string) {
	h.mu.Lock()
	h.auths[idx].json = doc
	h.mu.Unlock()
}

func (h *fakeHost) doc(t *testing.T, idx string) map[string]any {
	t.Helper()
	h.mu.Lock()
	raw := h.auths[idx].json
	h.mu.Unlock()
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("file %s is not JSON: %v (%s)", idx, err, raw)
	}
	return doc
}

func (h *fakeHost) respond(status int, body string) {
	h.httpResp, h.httpErr = hostHTTPResponse{StatusCode: status, Body: []byte(body)}, nil
}

// ---- harness ----------------------------------------------------------------------

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

const metaDoc = `{"type":"meta","dca_token":"dca-1","access_token":"old-key","api_key":"old-key","expired":"2026-01-01T00:00:00Z",` +
	`"base_url":"https://old.meta.ai","dca_expires_at":1893456000123,"custom":{"keep":[1,2]},"subs_tier_name":"old","subs_tier_id":"oldid"}`

const mintOK = `{"api_key":"new-key","base_url":"https://api.meta.ai/","user_email":"a@b.c","user_full_name":"Al","subs_tier_name":"Pro","is_subs_active":true}`

type healthEnv struct {
	store *SQLiteStore
	host  *fakeHost
	w     *healthWorker
	now   time.Time
}

func newHealthEnv(t *testing.T) *healthEnv {
	t.Helper()
	store := newTestStore(t)
	host := newFakeHost()
	host.respond(200, mintOK)
	e := &healthEnv{store: store, host: host, w: &healthWorker{store: store, host: host}, now: t0}
	prevHost, prevNow := testHealthHost, healthNow
	testHealthHost = host
	healthNow = func() time.Time { return e.now }
	healthSeen.reset(nil)
	t.Cleanup(func() {
		testHealthHost, healthNow = prevHost, prevNow
		healthSeen.reset(nil)
	})
	return e
}

func (e *healthEnv) observe(t *testing.T, rec Record, now time.Time) {
	t.Helper()
	observeUsage(context.Background(), e.store, rec, now)
}

func (e *healthEnv) row(t *testing.T, authID string) (healthRow, bool) {
	t.Helper()
	rows, err := e.store.healthRowsWhere(context.Background(), `WHERE auth_id = ?`, authID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		return healthRow{}, false
	}
	return rows[0], true
}

func (e *healthEnv) mustRow(t *testing.T, authID string) healthRow {
	t.Helper()
	r, ok := e.row(t, authID)
	if !ok {
		t.Fatalf("no health row for %s", authID)
	}
	return r
}

func (e *healthEnv) noRow(t *testing.T, authID string) {
	t.Helper()
	if r, ok := e.row(t, authID); ok {
		t.Fatalf("unexpected health row for %s: %+v", authID, r)
	}
}

func (e *healthEnv) due(t *testing.T, now time.Time) {
	t.Helper()
	e.now = now // the worker stamps its writes with healthNow()
	e.w.processDue(context.Background(), now)
}

func rec401(authID, idx string, ts time.Time) Record {
	return Record{ID: "r-" + authID + ts.Format("150405.000"), Timestamp: ts, Provider: "meta", AuthID: authID, AuthIndex: idx,
		Failed: true, FailureStatusCode: 401, FailureBody: "unauthorized"}
}

func recOK(authID, idx string, ts time.Time) Record {
	return Record{ID: "ok-" + authID + ts.Format("150405.000"), Timestamp: ts, Provider: "meta", AuthID: authID, AuthIndex: idx}
}

// pendingFlow opens a 401 for acct "a" at t0 and returns the env; the host has
// one active credential "idx-a" / "a.json".
func pendingFlow(t *testing.T) *healthEnv {
	t.Helper()
	e := newHealthEnv(t)
	e.host.add("idx-a", "a.json", metaDoc)
	e.observe(t, rec401("a", "idx-a", t0), t0)
	return e
}

// ---- ingestion --------------------------------------------------------------------

func TestObserve401OpensPendingAndOnlyMovesEarlier(t *testing.T) {
	e := newHealthEnv(t)
	e.observe(t, rec401("a", "idx-a", t0), t0)
	r := e.mustRow(t, "a")
	if r.State != healthPending || !r.CheckAt.Equal(t0.Add(healthCheckDelay)) || !r.Last401At.Equal(t0) || !r.First401At.Equal(t0) || r.HTTPStatus != 401 {
		t.Fatalf("row = %+v", r)
	}
	later := t0.Add(30 * time.Second)
	e.observe(t, rec401("a", "idx-a", later), later)
	r = e.mustRow(t, "a")
	if !r.CheckAt.Equal(t0.Add(healthCheckDelay)) {
		t.Fatalf("check_at moved later: %v", r.CheckAt)
	}
	if !r.Last401At.Equal(later) || !r.First401At.Equal(t0) {
		t.Fatalf("last/first = %v / %v", r.Last401At, r.First401At)
	}
}

func TestObserveIgnoresOtherTraffic(t *testing.T) {
	e := newHealthEnv(t)
	nonMeta := rec401("b", "idx-b", t0)
	nonMeta.Provider = "claude"
	notAuth := rec401("c", "idx-c", t0)
	notAuth.FailureStatusCode = 500
	noAuthID := rec401("", "idx-d", t0)
	for _, r := range []Record{nonMeta, notAuth, noAuthID} {
		e.observe(t, r, t0)
	}
	rows, err := e.store.healthRowsWhere(context.Background(), ``)
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows = %+v err=%v", rows, err)
	}
	// provider match is case-insensitive
	upper := rec401("e", "idx-e", t0)
	upper.Provider = " META "
	e.observe(t, upper, t0)
	e.mustRow(t, "e")
}

func TestObserveWakesDisabledRowWithoutChangingState(t *testing.T) {
	e := newHealthEnv(t)
	disabled := healthRow{AuthID: "a", AuthIndex: "idx-a", State: healthDisabled, Reason: reasonMintRejected, HTTPStatus: 403,
		Message: "orig", First401At: t0, Last401At: t0, UpdatedAt: t0}
	if err := e.store.saveDisabled(context.Background(), disabled); err != nil {
		t.Fatal(err)
	}
	healthSeen.add("a")
	later := t0.Add(time.Hour)
	e.observe(t, rec401("a", "idx-a", later), later)
	e.observe(t, recOK("a", "idx-a", t0.Add(2*time.Hour)), t0.Add(2*time.Hour)) // successes never clear a disabled row
	r := e.mustRow(t, "a")
	if r.State != healthDisabled || r.Reason != reasonMintRejected || r.Message != "orig" ||
		!r.Last401At.Equal(later) || !r.CheckAt.Equal(later.Add(healthCheckDelay)) {
		t.Fatalf("disabled row = %+v", r)
	}
}

func TestSuccessAfter401ClearsRow(t *testing.T) {
	e := pendingFlow(t)
	// stream that was sent before the 401 and ends later: proves nothing
	e.observe(t, recOK("a", "idx-a", t0.Add(-time.Second)), t0.Add(5*time.Second))
	e.mustRow(t, "a")
	// sent at/after the 401's receipt: recovered
	e.observe(t, recOK("a", "idx-a", t0), t0.Add(6*time.Second))
	e.noRow(t, "a")
}

func TestSuccessOfUntrackedAuthIsNoop(t *testing.T) {
	e := newHealthEnv(t)
	e.observe(t, recOK("a", "idx-a", t0), t0)
	e.noRow(t, "a")
}

// ---- worker: skip paths -----------------------------------------------------------

func TestWorkerDropsWhenCPARefreshedAfter401(t *testing.T) {
	e := pendingFlow(t)
	e.host.auths["idx-a"].lastRefresh = t0.Add(time.Second)
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "a")
	if len(e.host.requests) != 0 {
		t.Fatalf("mint called: %d", len(e.host.requests))
	}
}

// CPA keeps status "active" on a credential its rejected-key check still
// blocks, so the raw runtime status must never count as recovery.
func TestWorkerIgnoresRuntimeStatus(t *testing.T) {
	for _, a := range []fakeAuth{{status: "active"}, {status: " Active "}, {status: "active", unavailable: true}, {status: "error"}, {}} {
		e := pendingFlow(t)
		rt := e.host.auths["idx-a"]
		rt.status, rt.unavailable, rt.lastRefresh = a.status, a.unavailable, t0.Add(-time.Hour)
		e.due(t, t0.Add(healthCheckDelay))
		if len(e.host.requests) != 1 || e.mustRow(t, "a").State != healthMinted {
			t.Fatalf("%+v: expected a swap, requests=%d", a, len(e.host.requests))
		}
	}
}

func TestMintedActiveStatusStillDisables(t *testing.T) {
	e, _, at := mintedWith401(t)
	a := e.host.auths["idx-a"]
	a.status, a.lastRefresh = "active", at.Add(-time.Hour)
	e.due(t, at.Add(healthCheckDelay))
	if d := e.host.doc(t, "idx-a"); d["disabled"] != true || d["disabled_reason"] != reasonNewKeyRejected {
		t.Fatalf("doc = %v", d)
	}
	if len(e.host.resets) != 1 { // only the earlier successful swap
		t.Fatalf("resets = %v", e.host.resets)
	}
}

func TestWorkerNotDueIsUntouched(t *testing.T) {
	e := pendingFlow(t)
	e.due(t, t0.Add(healthCheckDelay-time.Second))
	if len(e.host.requests) != 0 || e.mustRow(t, "a").State != healthPending {
		t.Fatal("processed before due")
	}
}

func TestWorkerDropsManualDisableAndMissingAuth(t *testing.T) {
	e := pendingFlow(t)
	e.host.auths["idx-a"].disabled = true
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "a")

	e.observe(t, rec401("gone", "idx-gone", t0), t0)
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "gone")

	e.observe(t, rec401("noidx", "", t0), t0)
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "noidx")
	dropLogs := 0
	for _, l := range e.host.logs {
		if l.level == "info" && strings.HasPrefix(l.message, "stopped tracking a Muse credential: ") {
			dropLogs++
		}
	}
	if dropLogs != 3 || len(e.host.resets) != 0 {
		t.Fatalf("drop logs = %d resets = %v", dropLogs, e.host.resets)
	}
	if len(e.host.requests) != 0 {
		t.Fatal("mint called")
	}
}

func TestWorkerPostponesOnHostError(t *testing.T) {
	e := pendingFlow(t)
	e.host.runtimeErr = errors.New("host down")
	now := t0.Add(healthCheckDelay)
	e.due(t, now)
	r := e.mustRow(t, "a")
	if r.State != healthPending || r.Attempts != 0 || !r.CheckAt.Equal(now.Add(healthHostRetryDelay)) {
		t.Fatalf("row = %+v", r)
	}
}

// ---- worker: mint success ---------------------------------------------------------

func TestMintSuccessRewritesFile(t *testing.T) {
	t.Setenv("META_MINT_URL", "https://mint.test.meta.ai/key")
	e := pendingFlow(t)
	now := t0.Add(healthCheckDelay)
	e.due(t, now)

	if len(e.host.requests) != 1 {
		t.Fatalf("requests = %d", len(e.host.requests))
	}
	req := e.host.requests[0]
	if req.Method != "POST" || req.URL != "https://mint.test.meta.ai/key" {
		t.Fatalf("req = %+v", req)
	}
	if req.Headers["Authorization"][0] != "Bearer dca-1" || req.Headers["User-Agent"][0] != "muse-code/1.0.2" {
		t.Fatalf("headers = %v", req.Headers)
	}
	if string(req.Body) != `{"dca_token":"dca-1"}` {
		t.Fatalf("body = %s", req.Body)
	}

	doc := e.host.doc(t, "idx-a")
	want := map[string]any{
		"api_key": "new-key", "access_token": "new-key", "base_url": "https://api.meta.ai", "email": "a@b.c", "name": "Al",
		"subs_tier_name": "Pro", "is_subs_active": true, "has_payment_method": false, "type": "meta",
		"last_refresh": now.Format(time.RFC3339), "dca_token": "dca-1",
	}
	for k, v := range want {
		if doc[k] != v {
			t.Errorf("%s = %v, want %v", k, doc[k], v)
		}
	}
	if _, ok := doc["expired"]; ok {
		t.Error("expired not removed")
	}
	if _, ok := doc["subs_tier_id"]; ok {
		t.Error("subs_tier_id not deleted")
	}
	if _, ok := doc["disabled"]; ok {
		t.Error("file disabled")
	}
	raw := e.host.auths["idx-a"].json
	if !strings.Contains(raw, `"dca_expires_at":1893456000123`) || !strings.Contains(raw, `"custom":{"keep":[1,2]}`) {
		t.Fatalf("unrelated fields not preserved: %s", raw)
	}

	r := e.mustRow(t, "a")
	if r.State != healthMinted || !r.MintedAt.Equal(now) || !r.CheckAt.Equal(now.Add(mintedWatchWindow)) || r.Attempts != 0 || r.KeyFP == "" {
		t.Fatalf("row = %+v", r)
	}
	// The 401's model cooldown survives the credential change in CPA, so the
	// swap must reset it or the account stays idle for up to 30 minutes.
	if len(e.host.resets) != 1 || e.host.resets[0] != "idx-a" {
		t.Fatalf("cooldown resets = %v, want [idx-a]", e.host.resets)
	}
}

func TestMintDefaultURL(t *testing.T) {
	t.Setenv("META_MINT_URL", "")
	if got := mintRequest("x").URL; got != metaMintDefaultURL {
		t.Fatalf("url = %s", got)
	}
}

func TestMintBaseURLOnlyHTTPSMetaAI(t *testing.T) {
	cases := map[string]string{
		"https://api.meta.ai/":    "https://api.meta.ai",
		"https://meta.ai":         "https://meta.ai",
		"http://api.meta.ai":      "",
		"https://evil.com":        "",
		"https://meta.ai.evil.io": "",
		"https://notmeta.ai":      "",
		"":                        "",
	}
	for in, want := range cases {
		e := pendingFlow(t)
		e.host.respond(200, fmt.Sprintf(`{"api_key":"k","base_url":%q}`, in))
		e.due(t, t0.Add(healthCheckDelay))
		got, _ := e.host.doc(t, "idx-a")["base_url"].(string)
		if want == "" {
			want = "https://old.meta.ai" // untouched
		}
		if got != want {
			t.Errorf("base_url %q -> %q, want %q", in, got, want)
		}
	}
}

// ---- worker: rejection / failure --------------------------------------------------

func TestMintRejectedDisablesFile(t *testing.T) {
	for _, status := range []int{401, 403} {
		e := pendingFlow(t)
		e.host.respond(status, "nope")
		now := t0.Add(healthCheckDelay)
		e.due(t, now)
		doc := e.host.doc(t, "idx-a")
		if doc["disabled"] != true || doc["disabled_by"] != "usage-statistics" || doc["disabled_reason"] != reasonMintRejected ||
			doc["disabled_at"] != now.Format(time.RFC3339) {
			t.Fatalf("status %d doc = %v", status, doc)
		}
		if doc["api_key"] != "old-key" || len(e.host.resets) != 0 {
			t.Fatalf("credential changed or cooldown reset: %v %v", doc, e.host.resets)
		}
		r := e.mustRow(t, "a")
		if r.State != healthDisabled || r.Reason != reasonMintRejected || r.HTTPStatus != status || r.Message != "nope" || !r.CheckAt.IsZero() {
			t.Fatalf("row = %+v", r)
		}
		// disabled rows are never due again
		e.due(t, now.Add(24*time.Hour))
		if len(e.host.requests) != 1 {
			t.Fatalf("requests = %d", len(e.host.requests))
		}
	}
}

func TestMintFailuresRetryWithBackoffAndNeverDisable(t *testing.T) {
	cases := map[string]func(h *fakeHost){
		"503":         func(h *fakeHost) { h.respond(503, "busy") },
		"transport":   func(h *fakeHost) { h.httpErr = errors.New("dial tcp: timeout") },
		"no api_key":  func(h *fakeHost) { h.respond(200, `{"base_url":"https://api.meta.ai"}`) },
		"invalid":     func(h *fakeHost) { h.respond(200, `not json`) },
		"empty key":   func(h *fakeHost) { h.respond(200, `{"api_key":"  "}`) },
		"redirect300": func(h *fakeHost) { h.respond(302, ``) },
	}
	wantDelays := []time.Duration{5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour, time.Hour, time.Hour}
	for name, script := range cases {
		e := pendingFlow(t)
		script(e.host)
		now := t0.Add(healthCheckDelay)
		for i, delay := range wantDelays {
			e.due(t, now)
			r := e.mustRow(t, "a")
			if r.State != healthRetry || r.Attempts != i+1 || !r.CheckAt.Equal(now.Add(delay)) {
				t.Fatalf("%s attempt %d: row = %+v (want delay %v)", name, i+1, r, delay)
			}
			if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok || len(e.host.saves) != 0 || len(e.host.resets) != 0 {
				t.Fatalf("%s: file touched or cooldown reset", name)
			}
			now = r.CheckAt
		}
	}
}

func TestRetryThenSuccessMints(t *testing.T) {
	e := pendingFlow(t)
	e.host.respond(503, "busy")
	now := t0.Add(healthCheckDelay)
	e.due(t, now)
	e.host.respond(200, mintOK)
	now = e.mustRow(t, "a").CheckAt
	e.due(t, now)
	r := e.mustRow(t, "a")
	if r.State != healthMinted || r.Attempts != 0 {
		t.Fatalf("row = %+v", r)
	}
}

func TestNoDCATokenDisables(t *testing.T) {
	e := newHealthEnv(t)
	e.host.add("idx-a", "a.json", `{"type":"meta","api_key":"k","access_token":"plain"}`)
	e.observe(t, rec401("a", "idx-a", t0), t0)
	e.due(t, t0.Add(healthCheckDelay))
	doc := e.host.doc(t, "idx-a")
	if doc["disabled"] != true || doc["disabled_reason"] != reasonNoDCAToken {
		t.Fatalf("doc = %v", doc)
	}
	if r := e.mustRow(t, "a"); r.State != healthDisabled || r.Reason != reasonNoDCAToken {
		t.Fatalf("row = %+v", r)
	}
	if len(e.host.requests) != 0 || len(e.host.resets) != 0 {
		t.Fatal("mint or cooldown reset without token")
	}
}

func TestAccessTokenWithDCAPrefixIsDCAToken(t *testing.T) {
	e := newHealthEnv(t)
	e.host.add("idx-a", "a.json", `{"type":"meta","access_token":"dca:abc"}`)
	e.observe(t, rec401("a", "idx-a", t0), t0)
	e.due(t, t0.Add(healthCheckDelay))
	if len(e.host.requests) != 1 || e.host.requests[0].Headers["Authorization"][0] != "Bearer dca:abc" {
		t.Fatalf("requests = %+v", e.host.requests)
	}
	if e.mustRow(t, "a").State != healthMinted {
		t.Fatal("not minted")
	}
}

// ---- minted state -----------------------------------------------------------------

func mintedFlow(t *testing.T) (*healthEnv, time.Time) {
	t.Helper()
	e := pendingFlow(t)
	m := t0.Add(healthCheckDelay)
	e.due(t, m)
	if e.mustRow(t, "a").State != healthMinted {
		t.Fatal("setup: not minted")
	}
	return e, m
}

func TestMintedIgnoresStale401(t *testing.T) {
	e, m := mintedFlow(t)
	before := e.mustRow(t, "a")
	e.observe(t, rec401("a", "idx-a", m.Add(-time.Second)), m.Add(5*time.Second))
	after := e.mustRow(t, "a")
	if !after.Last401At.Equal(before.Last401At) || !after.CheckAt.Equal(before.CheckAt) {
		t.Fatalf("stale 401 recorded: %+v", after)
	}
}

// mintedWith401 returns a minted env plus the time of a later 401 on the new key.
func mintedWith401(t *testing.T) (*healthEnv, time.Time, time.Time) {
	t.Helper()
	e, m := mintedFlow(t)
	at := m.Add(10 * time.Second)
	e.observe(t, rec401("a", "idx-a", at), at)
	r := e.mustRow(t, "a")
	if !r.CheckAt.Equal(at.Add(healthCheckDelay)) || r.State != healthMinted {
		t.Fatalf("row = %+v", r)
	}
	return e, m, at
}

func TestMintedNewKeyRejectedDisables(t *testing.T) {
	// (b) CPA refreshed after minted_at but before the 401, status error: the
	// "Meta mints keys but rejects every one" loop guard.
	for name, refresh := range map[string]time.Time{"never": {}, "between": time.Time{}.Add(0)} {
		e, m, at := mintedWith401(t)
		if name == "between" {
			refresh = m.Add(5 * time.Second)
		}
		a := e.host.auths["idx-a"]
		a.lastRefresh, a.status = refresh, "error"
		e.due(t, at.Add(healthCheckDelay))
		doc := e.host.doc(t, "idx-a")
		if doc["disabled"] != true || doc["disabled_reason"] != reasonNewKeyRejected || doc["disabled_by"] != "usage-statistics" {
			t.Fatalf("%s: doc = %v", name, doc)
		}
		if r := e.mustRow(t, "a"); r.State != healthDisabled || r.Reason != reasonNewKeyRejected {
			t.Fatalf("%s: row = %+v", name, r)
		}
		if len(e.host.requests) != 1 {
			t.Fatalf("%s: extra mint", name)
		}
	}
}

func TestMintedNewKeyRejectedSurvivesCPAKeySwap(t *testing.T) {
	// CPA's own in-request swap rewrites api_key/access_token but keeps the
	// dca token: not a re-login, so it must not cancel the disable (seen in
	// the rehearsal).
	e, _, at := mintedWith401(t)
	doc := e.host.doc(t, "idx-a")
	doc["access_token"], doc["api_key"] = "LLM|cpa-swapped", "LLM|cpa-swapped"
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	e.host.setJSON("idx-a", string(raw))
	a := e.host.auths["idx-a"]
	a.lastRefresh, a.status = at.Add(-time.Second), "error"
	e.due(t, at.Add(healthCheckDelay))
	if d := e.host.doc(t, "idx-a"); d["disabled"] != true || d["disabled_reason"] != reasonNewKeyRejected {
		t.Fatalf("doc = %v", d)
	}
}

func TestMintedRecoveredIsDropped(t *testing.T) {
	cases := map[string]func(a *fakeAuth, at time.Time){
		"(c) refreshed after the 401": func(a *fakeAuth, at time.Time) { a.lastRefresh, a.status = at.Add(time.Second), "error" },
	}
	for name, setup := range cases {
		e, _, at := mintedWith401(t)
		setup(e.host.auths["idx-a"], at)
		e.due(t, at.Add(healthCheckDelay))
		e.noRow(t, "a")
		if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok || len(e.host.requests) != 1 {
			t.Fatalf("%s: disabled or minted again", name)
		}
	}
}

func TestMintedWatchWindowEndsWithoutNew401(t *testing.T) {
	e, m := mintedFlow(t)
	e.due(t, m.Add(mintedWatchWindow))
	e.noRow(t, "a")
	if len(e.host.requests) != 1 {
		t.Fatal("unexpected mint")
	}
}

func TestSuccessAfterMintClearsMintedRow(t *testing.T) {
	e, m := mintedFlow(t)
	// sent before the swap with the old key: no proof
	e.observe(t, recOK("a", "idx-a", m.Add(-time.Second)), m.Add(time.Second))
	e.mustRow(t, "a")
	e.observe(t, recOK("a", "idx-a", m.Add(time.Second)), m.Add(2*time.Second))
	e.noRow(t, "a")
}

// ---- fingerprint guard ------------------------------------------------------------

func TestFingerprintGuardOnInstall(t *testing.T) {
	e := pendingFlow(t)
	e.host.onHTTP = func() {
		e.host.setJSON("idx-a", `{"type":"meta","dca_token":"dca-NEW","access_token":"login-key"}`)
	}
	e.due(t, t0.Add(healthCheckDelay))
	if len(e.host.saves) != 0 {
		t.Fatalf("file written: %v", e.host.saves)
	}
	if doc := e.host.doc(t, "idx-a"); doc["access_token"] != "login-key" {
		t.Fatalf("doc = %v", doc)
	}
	e.noRow(t, "a")
}

func TestFingerprintGuardOnDisable(t *testing.T) {
	e := pendingFlow(t)
	e.host.respond(401, "rejected")
	e.host.onHTTP = func() { e.host.setJSON("idx-a", `{"type":"meta","dca_token":"dca-NEW"}`) }
	e.due(t, t0.Add(healthCheckDelay))
	if len(e.host.saves) != 0 {
		t.Fatalf("file written: %v", e.host.saves)
	}
	if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok {
		t.Fatal("re-logged-in credential disabled")
	}
	e.noRow(t, "a")
}

// ---- backfill & reconcile ---------------------------------------------------------

func insertMeta(t *testing.T, e *healthEnv, id, authID, idx, provider string, ts time.Time, status int) {
	t.Helper()
	insertAll(t, e.store, Record{ID: id, Timestamp: ts, Provider: provider, Model: "m", AuthID: authID, AuthIndex: idx,
		Failed: status >= 400, FailureStatusCode: status, FailureBody: "body-" + id})
}

func TestBackfill(t *testing.T) {
	e := newHealthEnv(t)
	now := t0
	e.now = now
	ctx := context.Background()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }

	// A: latest is 401, credential disabled with no marker
	insertMeta(t, e, "a1", "A", "idx-A", "meta", ago(3*time.Hour), 200)
	insertAll(t, e.store, Record{ID: "a2", Timestamp: ago(time.Hour), Provider: "meta", Model: "m", AuthID: "A", AuthIndex: "idx-A",
		LatencyMs: 1500, Failed: true, FailureStatusCode: 401, FailureBody: "body-a2"})
	e.host.add("idx-A", "A.json", `{"type":"meta","disabled":true,"x":1}`).disabled = true
	// B: latest 401, disabled by an admin already
	insertMeta(t, e, "b1", "B", "idx-B", "meta", ago(time.Hour), 401)
	e.host.add("idx-B", "B.json", `{"type":"meta","disabled":true,"disabled_by":"admin"}`).disabled = true
	// C: latest 401, active
	insertMeta(t, e, "c1", "C", "idx-C", "meta", ago(2*time.Hour), 401)
	e.host.add("idx-C", "C.json", metaDoc)
	// D: 401 then success: not a candidate
	insertMeta(t, e, "d1", "D", "idx-D", "meta", ago(2*time.Hour), 401)
	insertMeta(t, e, "d2", "D", "idx-D", "meta", ago(time.Hour), 200)
	e.host.add("idx-D", "D.json", metaDoc)
	// E: 401 older than the window
	insertMeta(t, e, "e1", "E", "idx-E", "meta", ago(backfillWindow+time.Hour), 401)
	e.host.add("idx-E", "E.json", metaDoc)
	// F: non-meta 401
	insertMeta(t, e, "f1", "F", "idx-F", "claude", ago(time.Hour), 401)
	e.host.add("idx-F", "F.json", metaDoc)
	// G: candidate the host no longer knows
	insertMeta(t, e, "g1", "G", "idx-G", "meta", ago(time.Hour), 401)

	if err := e.w.backfill(ctx); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	a := e.mustRow(t, "A")
	if a.State != healthDisabled || a.Reason != reasonBackfill || !a.Last401At.Equal(ago(time.Hour).Add(1500*time.Millisecond)) {
		t.Fatalf("A = %+v", a)
	}
	docA := e.host.doc(t, "idx-A")
	if docA["disabled_by"] != "usage-statistics" || docA["disabled_reason"] != reasonBackfill || docA["x"] != float64(1) {
		t.Fatalf("A doc = %v", docA)
	}
	e.noRow(t, "B") // disabled by someone else: not ours to adopt
	if e.host.doc(t, "idx-B")["disabled_by"] != "admin" {
		t.Fatal("existing disabled_by overwritten")
	}
	c := e.mustRow(t, "C")
	if c.State != healthPending || !c.CheckAt.Equal(now) || !c.Last401At.Equal(ago(2*time.Hour)) {
		t.Fatalf("C = %+v", c)
	}
	for _, id := range []string{"D", "E", "F", "G"} {
		if _, ok := e.row(t, id); ok {
			t.Fatalf("%s should not have a row", id)
		}
	}
	if done, _ := e.store.metaFlag(ctx, backfillMetaKey); !done {
		t.Fatal("flag not set")
	}

	// once only: a new candidate is not picked up by a second run
	insertMeta(t, e, "h1", "H", "idx-H", "meta", ago(time.Minute), 401)
	e.host.add("idx-H", "H.json", metaDoc)
	hits := e.host.runtimeHits
	if err := e.w.backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.row(t, "H"); ok || e.host.runtimeHits != hits {
		t.Fatal("backfill ran twice")
	}

	// the queued C row then goes through the normal worker
	e.due(t, now)
	if e.mustRow(t, "C").State != healthMinted {
		t.Fatal("C not minted")
	}
}

func TestBackfillHostErrorLeavesFlagUnset(t *testing.T) {
	e := newHealthEnv(t)
	insertMeta(t, e, "a1", "A", "idx-A", "meta", t0.Add(-time.Hour), 401)
	e.host.runtimeErr = errors.New("host down")
	if err := e.w.backfill(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if done, _ := e.store.metaFlag(context.Background(), backfillMetaKey); done {
		t.Fatal("flag set after failure")
	}
	// next start retries and succeeds
	e.host.runtimeErr = nil
	e.host.add("idx-A", "A.json", metaDoc)
	if err := e.w.backfill(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e.mustRow(t, "A").State != healthPending {
		t.Fatal("A not queued on retry")
	}
}

func TestReconcileDisabled(t *testing.T) {
	e := newHealthEnv(t)
	ctx := context.Background()
	for _, id := range []string{"X", "Y", "Z", "W"} {
		row := healthRow{AuthID: id, AuthIndex: "idx-" + id, State: healthDisabled, Reason: reasonMintRejected, UpdatedAt: t0}
		if err := e.store.saveDisabled(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	e.host.add("idx-X", "X.json", metaDoc)                             // re-enabled
	e.host.add("idx-Z", "Z.json", `{"disabled":true}`).disabled = true // still disabled
	e.host.add("idx-W", "W.json", metaDoc)
	// Y: file gone
	e.w.reconcileDisabled(ctx)
	e.noRow(t, "X")
	e.noRow(t, "Y")
	e.noRow(t, "W")
	if e.mustRow(t, "Z").State != healthDisabled {
		t.Fatal("Z dropped")
	}

	// a host error keeps everything
	e.host.runtimeErr = errors.New("host down")
	e.w.reconcileDisabled(ctx)
	e.mustRow(t, "Z")
}

// ---- last failure trigger and /health ---------------------------------------------

func TestLastFailureTriggerKeepsNewest(t *testing.T) {
	e := newHealthEnv(t)
	insertMeta(t, e, "f1", "A", "idx-A", "meta", t0, 500)
	insertMeta(t, e, "f2", "A", "idx-A", "meta", t0.Add(time.Minute), 401)
	insertMeta(t, e, "f3", "A", "idx-A", "meta", t0.Add(-time.Minute), 429)  // arrives late, older
	insertMeta(t, e, "f4", "A", "idx-A", "meta", t0.Add(2*time.Minute), 200) // success: ignored
	insertMeta(t, e, "f5", "", "idx-N", "meta", t0, 500)                     // no auth id
	got, err := e.store.lastFailures(context.Background(), t0.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].AuthID != "A" || got[0].StatusCode != 401 || !got[0].At.Equal(t0.Add(time.Minute)) || got[0].Message != "body-f2" {
		t.Fatalf("failures = %+v", got)
	}
}

func TestHealthEndpoint(t *testing.T) {
	e := newHealthEnv(t)
	useStore(t, e.store)
	prev := metaAutoDisable.Load()
	t.Cleanup(func() { metaAutoDisable.Store(prev) })

	call := func() (managementResponse, HealthResponse, string) {
		resp := callManagement(t, "GET", "/v0/management"+managementHealthPath, "")
		var out HealthResponse
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			t.Fatalf("body = %s err=%v", resp.Body, err)
		}
		return resp, out, string(resp.Body)
	}

	metaAutoDisable.Store(false)
	resp, out, body := call()
	if resp.StatusCode != 200 || out.AutoDisable || !strings.Contains(body, `"accounts":[]`) || !strings.Contains(body, `"last_failures":[]`) {
		t.Fatalf("empty body = %s", body)
	}

	e.now = t0.Add(2 * time.Hour)
	insertMeta(t, e, "n1", "new", "idx-new", "meta", e.now.Add(-time.Hour), 401)
	insertMeta(t, e, "o1", "old", "idx-old", "meta", e.now.Add(-30*time.Hour), 500)
	if err := e.store.insertHealth(context.Background(), healthRow{AuthID: "new", AuthIndex: "idx-new", State: healthPending,
		HTTPStatus: 401, Message: "m", First401At: t0, Last401At: t0, CheckAt: t0, UpdatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	metaAutoDisable.Store(true)
	_, out, _ = call()
	if !out.AutoDisable || len(out.Accounts) != 1 || out.Accounts[0].AuthID != "new" || out.Accounts[0].State != healthPending ||
		!out.Accounts[0].Since.Equal(t0) {
		t.Fatalf("accounts = %+v", out)
	}
	if len(out.LastFailures) != 1 || out.LastFailures[0].AuthID != "new" || out.LastFailures[0].StatusCode != 401 {
		t.Fatalf("last_failures (24h filter) = %+v", out.LastFailures)
	}

	if resp := callManagement(t, "POST", "/v0/management"+managementHealthPath, ""); resp.StatusCode != 405 {
		t.Fatalf("POST status = %d", resp.StatusCode)
	}
}

// ---- rpcHost ----------------------------------------------------------------------

func TestRPCHostInvoke(t *testing.T) {
	reply := func(raw string, err error) rpcHost {
		return rpcHost{call: func(string, []byte) ([]byte, error) { return []byte(raw), err }}
	}
	var out struct{ V int }

	if err := reply(`{"ok":true,"result":{"V":7}}`, nil).invoke("m", struct{}{}, &out); err != nil || out.V != 7 {
		t.Fatalf("ok: %v %+v", err, out)
	}
	if err := reply(`{"ok":true}`, nil).invoke("m", struct{}{}, &out); err != nil {
		t.Fatalf("ok without result: %v", err)
	}
	err := reply(`{"ok":false,"error":{"code":"x","message":"boom"}}`, nil).invoke("m", struct{}{}, nil)
	if err == nil || !strings.Contains(err.Error(), "boom") || errors.Is(err, errHostAuthNotFound) {
		t.Fatalf("error envelope: %v", err)
	}
	err = reply(`{"ok":false,"error":{"message":"Auth file Not Found"}}`, nil).invoke("m", struct{}{}, nil)
	if !errors.Is(err, errHostAuthNotFound) {
		t.Fatalf("not found: %v", err)
	}
	if err := reply(`{"ok":false}`, nil).invoke("host.x", struct{}{}, nil); err == nil || !strings.Contains(err.Error(), "host.x failed") {
		t.Fatalf("no message: %v", err)
	}
	if err := reply(`garbage`, nil).invoke("m", struct{}{}, nil); err == nil {
		t.Fatal("bad envelope accepted")
	}
	callErr := errors.New("transport")
	if err := reply(``, callErr).invoke("m", struct{}{}, nil); !errors.Is(err, callErr) {
		t.Fatalf("call error: %v", err)
	}

	// typed wrappers use the right method and payload
	var gotMethod string
	var gotPayload []byte
	h := rpcHost{call: func(method string, payload []byte) ([]byte, error) {
		gotMethod, gotPayload = method, payload
		return []byte(`{"ok":true,"result":{"auth":{"auth_index":"i","disabled":true,"last_refresh":"2026-06-01T12:00:00Z"}}}`), nil
	}}
	rt, err := h.authRuntime("i")
	if err != nil || gotMethod != "host.auth.get_runtime" || string(gotPayload) != `{"auth_index":"i"}` || !rt.Disabled || !rt.LastRefresh.Equal(t0) {
		t.Fatalf("authRuntime: %v %s %s %+v", err, gotMethod, gotPayload, rt)
	}
}

// ---- config & lifecycle -----------------------------------------------------------

func TestParseConfigMetaAutoDisable(t *testing.T) {
	cases := map[string]bool{
		"":                          false,
		"meta_auto_disable: true":   true,
		"meta_auto_disable: false":  false,
		`meta_auto_disable: "true"`: true,
		`meta_auto_disable: "no"`:   false,
		"Muse自动停用: true":            true,
		`Muse自动停用: "true"`:          true,
		"data_dir: /x":              false,
	}
	for yaml, want := range cases {
		if got := parseConfig([]byte(yaml)).MetaAutoDisable; got != want {
			t.Errorf("%q -> %v, want %v", yaml, got, want)
		}
	}
}

func TestStartHealthNoopWithoutSwitchOrHost(t *testing.T) {
	prevSwitch, prevHostFn := metaAutoDisable.Load(), hostCallFn
	prevHost := testHealthHost
	t.Cleanup(func() {
		metaAutoDisable.Store(prevSwitch)
		hostCallFn = prevHostFn
		testHealthHost = prevHost
		stopHealth()
	})
	useStore(t, newTestStore(t))
	started := func() bool {
		healthMu.Lock()
		defer healthMu.Unlock()
		return healthCancel != nil
	}

	testHealthHost, hostCallFn = newFakeHost(), nil
	metaAutoDisable.Store(false)
	startHealth()
	if started() || healthRunning.Load() {
		t.Fatal("started with the switch off")
	}

	testHealthHost, hostCallFn = nil, nil
	metaAutoDisable.Store(true)
	startHealth()
	if started() || healthRunning.Load() {
		t.Fatal("started without a host")
	}
	stopHealth() // idempotent
}

// ---- 401 end time, late records, success gate ---------------------------------------

func rec401Lat(authID, idx string, start time.Time, latencyMs int64) Record {
	r := rec401(authID, idx, start)
	r.ID += "-lat"
	r.LatencyMs = latencyMs
	return r
}

func TestObserve401UsesRequestEnd(t *testing.T) {
	e := newHealthEnv(t)
	arrive := t0.Add(9 * time.Second)
	e.now = arrive
	e.observe(t, rec401Lat("a", "idx-a", t0, 5000), arrive)
	r := e.mustRow(t, "a")
	if !r.Last401At.Equal(t0.Add(5*time.Second)) || !r.CheckAt.Equal(arrive.Add(healthCheckDelay)) || !r.UpdatedAt.Equal(arrive) {
		t.Fatalf("row = %+v", r)
	}
	// a shorter, earlier-ending 401 never lowers last_401_at
	e.observe(t, rec401Lat("a", "idx-a", t0, 1000), arrive)
	if r := e.mustRow(t, "a"); !r.Last401At.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("last_401_at lowered: %v", r.Last401At)
	}
	// no start time: falls back to the arrival time
	noStart := rec401("z", "idx-z", time.Time{})
	e.observe(t, noStart, arrive)
	if r := e.mustRow(t, "z"); !r.Last401At.Equal(arrive) {
		t.Fatalf("fallback last_401_at = %v", r.Last401At)
	}
}

func TestLateArrivingStale401IsRecovered(t *testing.T) {
	e := newHealthEnv(t)
	e.host.add("idx-a", "a.json", metaDoc)
	// CPA swapped the key at t0+10s; the 401 started at t0, ended at t0+2s and
	// was only delivered at t0+30s.
	e.host.auths["idx-a"].lastRefresh = t0.Add(10 * time.Second)
	arrive := t0.Add(30 * time.Second)
	e.observe(t, rec401Lat("a", "idx-a", t0, 2000), arrive)
	e.due(t, arrive.Add(healthCheckDelay))
	e.noRow(t, "a")
	if len(e.host.requests) != 0 || len(e.host.resets) != 0 {
		t.Fatal("swapped although CPA had already refreshed")
	}
}

func TestSwapRetry401AfterRefreshIsNotRecovered(t *testing.T) {
	e := newHealthEnv(t)
	e.host.add("idx-a", "a.json", metaDoc)
	e.host.auths["idx-a"].lastRefresh = t0.Add(10 * time.Second)
	// CPA: swap at t0+10s, retry started at t0+11s, 401 again.
	arrive := t0.Add(12 * time.Second)
	e.observe(t, rec401Lat("a", "idx-a", t0.Add(11*time.Second), 500), arrive)
	e.due(t, arrive.Add(healthCheckDelay))
	if len(e.host.requests) != 1 || e.mustRow(t, "a").State != healthMinted {
		t.Fatalf("expected a swap, requests=%d", len(e.host.requests))
	}
}

func TestSuccessGateUsesStartVsEnd(t *testing.T) {
	e := newHealthEnv(t)
	e.observe(t, rec401Lat("a", "idx-a", t0, 5000), t0.Add(5*time.Second)) // ends t0+5s
	e.observe(t, recOK("a", "idx-a", t0.Add(2*time.Second)), t0.Add(9*time.Second))
	e.mustRow(t, "a") // started before the 401 ended
	e.observe(t, recOK("a", "idx-a", t0.Add(5*time.Second)), t0.Add(9*time.Second))
	e.noRow(t, "a")
}

func TestSuccessGateMinted(t *testing.T) {
	e, _, at := mintedWith401(t) // 401 ended at `at`
	e2 := rec401Lat("a", "idx-a", at, 3000)
	e2.ID += "-m"
	e.observe(t, e2, at.Add(3*time.Second)) // ends at+3s
	e.observe(t, recOK("a", "idx-a", at.Add(time.Second)), at.Add(10*time.Second))
	e.mustRow(t, "a")
	e.observe(t, recOK("a", "idx-a", at.Add(3*time.Second)), at.Add(10*time.Second))
	e.noRow(t, "a")
}

func TestFingerprintIgnoresCPAKeySwapDuringMint(t *testing.T) {
	e := pendingFlow(t)
	e.host.onHTTP = func() {
		e.host.setJSON("idx-a", `{"type":"meta","dca_token":"dca-1","access_token":"LLM|cpa","api_key":"LLM|cpa"}`)
	}
	e.due(t, t0.Add(healthCheckDelay))
	if len(e.host.saves) != 1 || e.host.doc(t, "idx-a")["api_key"] != "new-key" {
		t.Fatalf("saves = %v doc = %v", e.host.saves, e.host.doc(t, "idx-a"))
	}
}

// ---- concurrency / lifecycle (version-guarded writes) -------------------------------

func countLogs(h *fakeHost, prefix string) int {
	n := 0
	for _, l := range h.logs {
		if strings.HasPrefix(l.message, prefix) {
			n++
		}
	}
	return n
}

// A 401 landing between the worker's read and its write bumps the row version:
// the worker's retry/drop/disable then does nothing and the row stays due.
func TestLostUpdateRetryIsDiscarded(t *testing.T) {
	e := pendingFlow(t)
	e.host.respond(503, "busy")
	due := t0.Add(healthCheckDelay)
	before := e.mustRow(t, "a")
	e.host.onHTTP = func() { e.observe(t, rec401("a", "idx-a", due), due) }
	e.due(t, due)
	r := e.mustRow(t, "a")
	if r.State != healthPending || r.Attempts != 0 || r.Version <= before.Version || r.CheckAt.After(due) {
		t.Fatalf("row = %+v", r)
	}
	if countLogs(e.host, "Muse key swap failed") != 0 {
		t.Fatal("retry was applied")
	}
	// still due: processed again, and now the swap works
	e.host.onHTTP = nil
	e.host.respond(200, mintOK)
	e.due(t, due)
	if e.mustRow(t, "a").State != healthMinted {
		t.Fatal("row not processed again")
	}
}

func TestLostUpdateDropIsDiscarded(t *testing.T) {
	e := pendingFlow(t)
	due := t0.Add(healthCheckDelay)
	e.host.onHTTP = func() {
		e.host.setJSON("idx-a", `{"type":"meta","dca_token":"dca-NEW"}`) // re-login → install would drop
		e.observe(t, rec401("a", "idx-a", due), due)                     // ... but a fresh 401 arrived too
	}
	e.due(t, due)
	if e.mustRow(t, "a").State != healthPending || len(e.host.saves) != 0 {
		t.Fatal("drop applied over a newer 401")
	}
}

func TestLostUpdateDisableIsDiscarded(t *testing.T) {
	e := pendingFlow(t)
	e.host.respond(401, "rejected")
	due := t0.Add(healthCheckDelay)
	e.host.onHTTP = func() { e.observe(t, rec401("a", "idx-a", due), due) }
	e.due(t, due)
	if len(e.host.saves) != 0 {
		t.Fatalf("file written: %v", e.host.saves)
	}
	if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok || e.mustRow(t, "a").State != healthPending {
		t.Fatal("disabled although the row changed meanwhile")
	}
}

func TestStale401AfterInstallIgnoredAcrossFreshWorker(t *testing.T) {
	e, m := mintedFlow(t)
	healthSeen.reset(nil) // no in-memory state survives: simulate a restart
	e.w = &healthWorker{store: e.store, host: e.host}
	before := e.mustRow(t, "a")
	e.observe(t, rec401Lat("a", "idx-a", m.Add(-30*time.Second), 20000), m.Add(30*time.Second))
	after := e.mustRow(t, "a")
	if after.Version != before.Version || !after.Last401At.Equal(before.Last401At) {
		t.Fatalf("stale 401 recorded: %+v", after)
	}
	e.due(t, m.Add(mintedWatchWindow))
	e.noRow(t, "a")
	if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok {
		t.Fatal("disabled by a stale 401")
	}
}

func TestStale401BeforeInstallIsCappedAtMintedAt(t *testing.T) {
	e := pendingFlow(t)
	due := t0.Add(healthCheckDelay)
	// sent with the old key, ends after the swap lands
	e.host.onHTTP = func() { e.observe(t, rec401Lat("a", "idx-a", due.Add(-time.Minute), 90000), due) }
	e.due(t, due)
	r := e.mustRow(t, "a")
	if r.State != healthMinted || !r.Last401At.Equal(due) || !r.MintedAt.Equal(due) {
		t.Fatalf("row = %+v", r)
	}
	e.due(t, due.Add(mintedWatchWindow))
	e.noRow(t, "a")
	if _, ok := e.host.doc(t, "idx-a")["disabled"]; ok {
		t.Fatal("new_key_rejected from a pre-install 401")
	}
}

func TestSuccessDuringMintSkipsInstall(t *testing.T) {
	e := pendingFlow(t)
	e.host.onHTTP = func() { e.observe(t, recOK("a", "idx-a", t0.Add(time.Minute)), t0.Add(time.Minute)) }
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "a")
	if len(e.host.saves) != 0 || len(e.host.resets) != 0 {
		t.Fatalf("installed after recovery: saves=%v resets=%v", e.host.saves, e.host.resets)
	}
}

func TestSuccessDuringRejectedMintDoesNotDisable(t *testing.T) {
	e := pendingFlow(t)
	e.host.respond(401, "rejected")
	e.host.onHTTP = func() { e.observe(t, recOK("a", "idx-a", t0.Add(time.Minute)), t0.Add(time.Minute)) }
	e.due(t, t0.Add(healthCheckDelay))
	e.noRow(t, "a")
	if len(e.host.saves) != 0 {
		t.Fatalf("file written: %v", e.host.saves)
	}
}

func TestAdminDisableDuringMint(t *testing.T) {
	for _, status := range []int{200, 401} {
		e := pendingFlow(t)
		e.host.respond(status, mintOK)
		e.host.onHTTP = func() {
			a := e.host.auths["idx-a"]
			a.disabled = true
			e.host.setJSON("idx-a", strings.Replace(metaDoc, `{"type":"meta",`, `{"type":"meta","disabled":true,`, 1))
		}
		e.due(t, t0.Add(healthCheckDelay))
		doc := e.host.doc(t, "idx-a")
		if _, ok := doc["disabled_by"]; ok || len(e.host.saves) != 0 || len(e.host.resets) != 0 {
			t.Fatalf("status %d: wrote over a manual disable: %v", status, doc)
		}
		e.noRow(t, "a")
	}
}

// disabledByUs drives a row to disabled (mint 401) and returns the env.
func disabledByUs(t *testing.T) *healthEnv {
	t.Helper()
	e := pendingFlow(t)
	e.host.respond(401, "rejected")
	e.due(t, t0.Add(healthCheckDelay))
	if e.mustRow(t, "a").State != healthDisabled {
		t.Fatal("setup: not disabled")
	}
	return e
}

// reEnable mimics an admin flipping `disabled` off while the marker fields stay.
func reEnable(t *testing.T, e *healthEnv) {
	t.Helper()
	doc := e.host.doc(t, "idx-a")
	doc["disabled"] = false
	raw, _ := json.Marshal(doc)
	e.host.setJSON("idx-a", string(raw))
	e.host.auths["idx-a"].disabled = false
}

func TestWokenDisabledRowStaysAsleepWhileStillDisabled(t *testing.T) {
	e := disabledByUs(t)
	at := t0.Add(time.Hour)
	e.observe(t, rec401("a", "idx-a", at), at)
	if e.mustRow(t, "a").CheckAt.IsZero() {
		t.Fatal("401 did not wake the disabled row")
	}
	saves := len(e.host.saves)
	e.due(t, at.Add(healthCheckDelay))
	r := e.mustRow(t, "a")
	if r.State != healthDisabled || !r.CheckAt.IsZero() || len(e.host.saves) != saves || len(e.host.requests) != 1 {
		t.Fatalf("row = %+v saves=%d requests=%d", r, len(e.host.saves), len(e.host.requests))
	}
}

func TestWokenDisabledRowRestartsRoundWhenEnabled(t *testing.T) {
	e := disabledByUs(t)
	reEnable(t, e)
	e.host.respond(200, mintOK)
	at := t0.Add(time.Hour)
	e.observe(t, rec401("a", "idx-a", at), at)
	e.due(t, at.Add(healthCheckDelay))
	doc := e.host.doc(t, "idx-a")
	for _, f := range markerFields {
		if _, ok := doc[f]; ok {
			t.Errorf("marker %s not stripped", f)
		}
	}
	if doc["api_key"] != "new-key" || doc["disabled"] == true {
		t.Fatalf("doc = %v", doc)
	}
	r := e.mustRow(t, "a")
	if r.State != healthMinted || r.Reason != "" || len(e.host.requests) != 2 {
		t.Fatalf("row = %+v requests=%d", r, len(e.host.requests))
	}
}

func TestReconcileStripsMarkerFromEnabledFile(t *testing.T) {
	e := disabledByUs(t)
	reEnable(t, e)
	e.w.reconcileDisabled(context.Background())
	doc := e.host.doc(t, "idx-a")
	for _, f := range markerFields {
		if _, ok := doc[f]; ok {
			t.Errorf("marker %s left in an enabled file", f)
		}
	}
	e.noRow(t, "a")
}

func TestReconcileKeepsRowWhenStripFails(t *testing.T) {
	e := disabledByUs(t)
	reEnable(t, e)
	e.host.setJSON("idx-a", `not json`) // readFile fails → cannot confirm a clean file
	e.w.reconcileDisabled(context.Background())
	e.mustRow(t, "a")
}

func TestBackfillRetriesUntilHostIsReady(t *testing.T) {
	e := newHealthEnv(t)
	ctx := context.Background()
	insertMeta(t, e, "a1", "A", "idx-A", "meta", t0.Add(-time.Hour), 401)
	e.host.add("idx-A", "A.json", metaDoc)
	e.host.runtimeErr = errors.New("core auth manager unavailable")
	for i := 0; i < 2; i++ {
		if err := e.w.backfill(ctx); err == nil {
			t.Fatal("expected error while the host is not ready")
		}
		if done, _ := e.store.metaFlag(ctx, backfillMetaKey); done {
			t.Fatal("flag set early")
		}
	}
	e.host.runtimeErr = nil
	if err := e.w.backfill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.w.backfill(ctx); err != nil { // flag set: no-op
		t.Fatal(err)
	}
	if countLogs(e.host, "auto-disable backfill done") != 1 {
		t.Fatalf("done logged %d times", countLogs(e.host, "auto-disable backfill done"))
	}
	// the queued row is picked up by a normal tick
	e.due(t, t0)
	if e.mustRow(t, "A").State != healthMinted {
		t.Fatal("queued row not processed")
	}
}

func TestBackfillDoesNotOverrideLiveRow(t *testing.T) {
	e := newHealthEnv(t)
	insertMeta(t, e, "a1", "A", "idx-A", "meta", t0.Add(-time.Hour), 401)
	e.host.add("idx-A", "A.json", metaDoc)
	e.observe(t, rec401("A", "idx-A", t0.Add(-time.Minute)), t0.Add(-time.Minute)) // live check already open
	before := e.mustRow(t, "A")
	if err := e.w.backfill(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := e.mustRow(t, "A"); after.Version != before.Version || !after.CheckAt.Equal(before.CheckAt) {
		t.Fatalf("live row overridden: %+v", after)
	}
}

func TestHealthEndpointSwitchOffHidesActiveChecks(t *testing.T) {
	e := newHealthEnv(t)
	useStore(t, e.store)
	prev := metaAutoDisable.Load()
	t.Cleanup(func() { metaAutoDisable.Store(prev) })
	ctx := context.Background()
	for id, state := range map[string]string{"p": healthPending, "r": healthRetry, "m": healthMinted} {
		if err := e.store.insertHealth(ctx, healthRow{AuthID: id, AuthIndex: "i-" + id, State: state, CheckAt: t0, UpdatedAt: t0}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.store.saveDisabled(ctx, healthRow{AuthID: "d", AuthIndex: "i-d", Reason: reasonMintRejected, UpdatedAt: t0}); err != nil {
		t.Fatal(err)
	}
	insertMeta(t, e, "f1", "p", "i-p", "meta", t0, 401)
	e.now = t0.Add(time.Hour)

	get := func() HealthResponse {
		resp := callManagement(t, "GET", "/v0/management"+managementHealthPath, "")
		var out HealthResponse
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	metaAutoDisable.Store(false)
	out := get()
	if out.AutoDisable || len(out.Accounts) != 1 || out.Accounts[0].AuthID != "d" || out.Accounts[0].State != healthDisabled {
		t.Fatalf("switch off: %+v", out)
	}
	if len(out.LastFailures) != 1 {
		t.Fatalf("last_failures should stay: %+v", out.LastFailures)
	}
	metaAutoDisable.Store(true)
	if out := get(); len(out.Accounts) != 4 {
		t.Fatalf("switch on: %+v", out.Accounts)
	}
}

func TestBackfillSkipsBadCredentialAndStillFinishes(t *testing.T) {
	e := newHealthEnv(t)
	ctx := context.Background()
	insertMeta(t, e, "a1", "A", "idx-A", "meta", t0.Add(-time.Hour), 401)
	insertMeta(t, e, "b1", "B", "idx-B", "meta", t0.Add(-time.Hour), 401)
	e.host.add("idx-A", "A.json", `not json`).disabled = true // unreadable file
	e.host.add("idx-B", "B.json", `{"type":"meta","disabled":true}`).disabled = true

	if err := e.w.backfill(ctx); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	e.noRow(t, "A")
	if r := e.mustRow(t, "B"); r.State != healthDisabled || r.Reason != reasonBackfill {
		t.Fatalf("B = %+v", r)
	}
	if doc := e.host.doc(t, "idx-B"); doc["disabled_by"] != "usage-statistics" || doc["disabled_reason"] != reasonBackfill {
		t.Fatalf("B doc = %v", doc)
	}
	warns := 0
	for _, l := range e.host.logs {
		if l.level == "warn" && strings.HasPrefix(l.message, "auto-disable backfill skipped a credential: ") {
			warns++
		}
	}
	if warns != 1 {
		t.Fatalf("skip warnings = %d", warns)
	}
	if done, _ := e.store.metaFlag(ctx, backfillMetaKey); !done {
		t.Fatal("flag not set")
	}
	hits := e.host.runtimeHits
	if err := e.w.backfill(ctx); err != nil || e.host.runtimeHits != hits {
		t.Fatalf("second backfill not a no-op: err=%v", err)
	}
}
