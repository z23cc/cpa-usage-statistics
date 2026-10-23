package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// pluginConfig holds the plugin-owned settings read from
// plugins.configs.usage-statistics.
type pluginConfig struct {
	DataDir       string
	RetentionDays int
	// MetaAutoDisable turns on the Muse 401 key swap / auto-disable (health.go).
	MetaAutoDisable bool
}

// parseConfig reads the plugin config YAML block, accepting English keys and
// Chinese aliases.
func parseConfig(raw []byte) pluginConfig {
	cfg := pluginConfig{}
	if len(raw) == 0 {
		return cfg
	}
	var m map[string]any
	if err := yaml.Unmarshal(raw, &m); err != nil || m == nil {
		return cfg
	}
	if v, ok := lookupConfig(m, "data_dir", "数据目录"); ok {
		cfg.DataDir = strings.TrimSpace(toConfigString(v))
	}
	if v, ok := lookupConfig(m, "retention_days", "用量保留天数"); ok {
		cfg.RetentionDays = toConfigInt(v)
	}
	if v, ok := lookupConfig(m, "meta_auto_disable", "Muse自动停用"); ok {
		cfg.MetaAutoDisable = toConfigBool(v)
	}
	return cfg
}

func toConfigBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int:
		return b != 0
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(b))
		return err == nil && parsed
	}
	return false
}

// resolveDBPath resolves the usage.db path: config data_dir, then the
// USAGE_STATISTICS_DIR env var, then a cross-platform home-based default.
func resolveDBPath(cfg pluginConfig) (string, error) {
	dir := strings.TrimSpace(cfg.DataDir)
	if dir == "" {
		dir = strings.TrimSpace(os.Getenv("USAGE_STATISTICS_DIR"))
	}
	if dir == "" {
		if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
			dir = filepath.Join(home, ".cli-proxy-api", "plugins", pluginID)
		} else {
			dir = filepath.Join(".cli-proxy-api", "plugins", pluginID)
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("usage-statistics: create data dir: %w", err)
	}
	return filepath.Join(dir, "usage.db"), nil
}

func lookupConfig(m map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			return v, true
		}
	}
	return nil, false
}

func toConfigString(v any) string {
	switch s := v.(type) {
	case nil:
		return ""
	case string:
		return s
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toConfigInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
			return i
		}
	}
	return 0
}
