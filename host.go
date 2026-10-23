package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// pluginHost is the subset of CPA host callbacks the Muse auto-disable uses.
// rpcHost talks to the real host; tests substitute a fake.
type pluginHost interface {
	authRuntime(authIndex string) (hostAuthRuntime, error)
	authGet(authIndex string) (hostAuthFile, error)
	authSave(name string, data []byte) error
	resetCooldown(authIndex string) error
	httpDo(ctx context.Context, req hostHTTPRequest) (hostHTTPResponse, error)
	log(level, message string, fields map[string]any)
}

// hostAuthRuntime is the part of pluginapi.HostAuthFileEntry read here.
// LastRefresh is CPA's in-memory LastRefreshedAt: it only moves when CPA
// itself refreshes the credential, not when a file is saved. Status and
// Unavailable are only logged: they are CPA's raw fields, which stay
// "active"/false while a rejected key still blocks the credential.
type hostAuthRuntime struct {
	AuthIndex   string    `json:"auth_index"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Disabled    bool      `json:"disabled"`
	Unavailable bool      `json:"unavailable"`
	LastRefresh time.Time `json:"last_refresh"`
}

type hostAuthFile struct {
	Name string          `json:"name"`
	JSON json.RawMessage `json:"json"`
}

type hostHTTPRequest struct {
	OperationID string              `json:"operation_id,omitempty"`
	Method      string              `json:"method"`
	URL         string              `json:"url"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        []byte              `json:"body,omitempty"`
}

// hostHTTPResponse mirrors pluginapi.HTTPResponse, which has no json tags.
type hostHTTPResponse struct {
	StatusCode int
	Headers    map[string][]string
	Body       []byte
}

// errHostAuthNotFound reports a credential the host no longer knows.
var errHostAuthNotFound = errors.New("auth not found")

// hostCallFn is set by the cgo glue when the host hands over its callback
// table; nil means there is no host (tests, non-cgo builds).
var hostCallFn func(method string, payload []byte) ([]byte, error)

type rpcHost struct {
	call func(method string, payload []byte) ([]byte, error)
}

func (h rpcHost) invoke(method string, req, out any) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("encode %s: %w", method, err)
	}
	raw, err := h.call(method, payload)
	if err != nil {
		return err
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode %s: %w", method, err)
	}
	if !env.OK {
		message := method + " failed"
		if env.Error != nil && strings.TrimSpace(env.Error.Message) != "" {
			message = env.Error.Message
		}
		if strings.Contains(strings.ToLower(message), "not found") {
			return fmt.Errorf("%s: %w: %s", method, errHostAuthNotFound, message)
		}
		return fmt.Errorf("%s: %s", method, message)
	}
	if out == nil || len(env.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Result, out); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}
	return nil
}

func (h rpcHost) authRuntime(authIndex string) (hostAuthRuntime, error) {
	var out struct {
		Auth hostAuthRuntime `json:"auth"`
	}
	err := h.invoke("host.auth.get_runtime", map[string]string{"auth_index": authIndex}, &out)
	return out.Auth, err
}

func (h rpcHost) authGet(authIndex string) (hostAuthFile, error) {
	var out hostAuthFile
	err := h.invoke("host.auth.get", map[string]string{"auth_index": authIndex}, &out)
	return out, err
}

func (h rpcHost) authSave(name string, data []byte) error {
	return h.invoke("host.auth.save", struct {
		Name string          `json:"name"`
		JSON json.RawMessage `json:"json"`
	}{Name: name, JSON: data}, nil)
}

// resetCooldown clears the credential's routing cooldowns, like
// POST /v8/management/routing/cooldown/reset.
func (h rpcHost) resetCooldown(authIndex string) error {
	return h.invoke("host.routing.reset_cooldown", map[string]string{"auth_index": authIndex}, nil)
}

// httpDo runs one request on the host's HTTP client (same egress and proxy
// settings as CPA's own calls) inside an operation, so ctx can cancel it.
func (h rpcHost) httpDo(ctx context.Context, req hostHTTPRequest) (hostHTTPResponse, error) {
	var op struct {
		OperationID string `json:"operation_id"`
	}
	if err := h.invoke("host.http.operation_open", struct{}{}, &op); err != nil {
		return hostHTTPResponse{}, err
	}
	finished, watcherDone := make(chan struct{}), make(chan struct{})
	// Joined before returning, so no host call outlives httpDo (and the worker).
	defer func() {
		close(finished)
		<-watcherDone
	}()
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = h.invoke("host.http.cancel", map[string]string{"operation_id": op.OperationID}, nil)
		case <-finished:
		}
	}()
	req.OperationID = op.OperationID
	var resp hostHTTPResponse
	if err := h.invoke("host.http.do", req, &resp); err != nil {
		if ctx.Err() != nil {
			return hostHTTPResponse{}, ctx.Err()
		}
		return hostHTTPResponse{}, err
	}
	return resp, nil
}

func (h rpcHost) log(level, message string, fields map[string]any) {
	_ = h.invoke("host.log", map[string]any{"level": level, "message": "usage-statistics: " + message, "fields": fields}, nil)
}
