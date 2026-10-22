# CPA Usage Statistics

CLIProxyAPI 的持久化用量统计插件。记录每次请求的用量，写入本地 SQLite；提供查询/删除接口。字段命名对齐上游 `usage.Record` / `usage.Detail`。

当前版本：`0.3.0-acct.1`（在 `0.1.0` 基础上增加按账号汇总、按账号分页明细、单价与花费，以及面板侧边栏里的"账号用量"页面）

## 建议使用配套的前端面板

[配套面板](https://github.com/Fwindy/Cli-Proxy-API-Management-Center)

## 功能

- 接收上游用量记录并持久化到 SQLite。
- 提供受管理鉴权保护的查询/删除接口。
- 可选按天数保留清理。

## 安装

下载对应平台的 release zip，将动态库放到 CLIProxyAPI 的插件目录：

```text
plugins/linux/amd64/usage-statistics.so
plugins/windows/amd64/usage-statistics.dll
plugins/darwin/arm64/usage-statistics.dylib
```

放好后重启 CLIProxyAPI。

## 配置

```yaml
plugins:
  enabled: true
  configs:
    usage-statistics:
      enabled: true
      priority: 100
      # 可选：usage.db 存放目录；缺省为 ~/.cli-proxy-api/plugins/usage-statistics
      data_dir: ""
      # 可选：保留天数，超过则清理旧记录；0 表示不清理
      retention_days: 0
```

`data_dir` 也可用环境变量 `USAGE_STATISTICS_DIR` 指定。中文配置键 `用量保留天数`、`数据目录` 同样可用。

## API

均为受管理鉴权保护的管理路由（宿主自动加 `/v0/management` 前缀）：

### 查询

```
GET /v0/management/plugins/usage-statistics/usage?start=<RFC3339>&end=<RFC3339>
```

响应按「分组键（api_key 或 provider）→ 模型」两层聚合：

```jsonc
{
  "<api_key 或 provider>": {
    "<model>": [
      {
        "id": "…",
        "timestamp": "2026-05-02T10:30:00Z",
        "provider": "anthropic",
        "source": "claude-code",
        "auth_index": "auth-1",
        "reasoning_effort": "high",
        "service_tier": "priority",
        "latency_ms": 1250,
        "ttft_ms": 200,
        "tokens": {
          "input_tokens": 10, "output_tokens": 20, "reasoning_tokens": 3,
          "cached_tokens": 4, "cache_read_tokens": 0, "cache_creation_tokens": 0,
          "total_tokens": 33
        },
        "failed": true,
        "failure_status_code": 429,
        "failure_body": "rate limited"
      }
    ]
  }
}
```

### 删除

```
DELETE /v0/management/plugins/usage-statistics/usage
Content-Type: application/json

{"ids": ["<record-id>", "..."]}
```

响应：`{"deleted": 1, "missing": ["..."]}`。

### 按账号汇总

```
GET /v0/management/plugins/usage-statistics/accounts?start=<RFC3339>&end=<RFC3339>
```

按凭证（`auth_index` + `auth_id`）聚合，按 `total_tokens` 降序。`start` 含、`end` 不含，均可省略。

```jsonc
{
  "accounts": [
    {
      "auth_index": "0a1b2c3d4e5f6a7b",
      "auth_id": "meta-user_example.com-0123456789abcdef.json",
      "source": "user@example.com",
      "provider": "meta",
      "auth_type": "oauth",
      "requests": 17, "success": 16, "failed": 1,
      "tokens": {
        "input_tokens": 8896, "output_tokens": 44867, "reasoning_tokens": 23919,
        "cached_tokens": 8000, "cache_read_tokens": 0, "cache_creation_tokens": 0,
        "total_tokens": 53763
      },
      "first_request_at": "2026-10-22T06:40:53.1Z",
      "last_request_at": "2026-10-22T06:48:37.2Z",
      "cost_usd": 1.12999,
      "unpriced_models": []
    }
  ],
  "totals": { "accounts": 1, "requests": 17, "success": 16, "failed": 1, "tokens": { "...": 0 }, "cost_usd": 1.12999 }
}
```

`cost_usd` 只计已设单价的模型；没设单价的模型列在 `unpriced_models` 里。

`total_tokens` 是该账号所有记录 `total_tokens` 之和（缓存计入输入、推理计入输出，不要再相加）。汇总要扫描范围内所有记录（约 1 秒 / 50 万条），数据量大时请带 `start`/`end` 或配置 `retention_days`。

### 按账号分页明细

```
GET /v0/management/plugins/usage-statistics/records?auth_index=<auth_index>&start=<RFC3339>&end=<RFC3339>&limit=100&cursor=<next_cursor>
```

- 按时间倒序（`timestamp DESC, id DESC`）。`auth_index`、`auth_id`、`start`、`end` 均为可选过滤条件。按账号查询请带 `auth_index`（`/accounts` 返回值里有），只带 `auth_id` 没有索引，数据量大时会慢。
- `limit` 缺省 100，最大 1000（超过按 1000 处理），非正整数返回 400。
- 首页不带 `cursor`；把上一页的 `next_cursor` 原样传回取下一页；`next_cursor` 为 `null` 表示最后一页。无效 `cursor` 返回 400。

```jsonc
{
  "records": [ { "id": "…", "timestamp": "…", "auth_index": "…", "model": "…", "tokens": { "...": 0 }, "failed": false, "cost_usd": 0.0712 } ],
  "next_cursor": "MjAyNi0xMC0yMlQwNjo0MD…"
}
```

`timestamp` 是请求开始时间，记录在请求完成后才写入，所以耗时长的请求会出现在已翻过的位置之后：游标适合浏览，不适合做增量同步的水位线。每条记录的 `cost_usd` 在该模型没设单价时为 `null`。

### 单价

```
GET /v0/management/plugins/usage-statistics/prices
PUT /v0/management/plugins/usage-statistics/prices
Content-Type: application/json

{"prices": [{"model": "muse-wonder-full-1.0", "input": 5, "output": 25, "cache_read": 0.2, "cache_creation": 0}]}
```

- 单位是美元 / 百万 token，与配套面板相同。`GET` 另外返回 `models_seen`，即记录里出现过的模型。
- `PUT` 整体替换单价表，传 `[]` 表示清空；请求体缺少 `prices` 键时返回 400，防止误清空。每个价须在 0–10000 之间，最多 500 个模型。
- `PUT` 只接受 `{"prices": [...]}`，出现未知字段（例如面板里的 `cacheRead` 写法，或 `GET` 返回的 `models_seen`）一律返回 400，避免字段名写错时被当成 0 存进去。
- 单价存在 `usage.db` 的 `model_prices` 表里。旧版本插件会忽略这张表，回滚后照样能打开数据库。

### 花费口径

与配套面板的 `calculateCost` 一致，但不含上下文阶梯价和 Tier 倍率：

```
纯输入 = provider 含 claude/anthropic ? input_tokens : max(input_tokens − cached_tokens − cache_creation_tokens, 0)
花费   = (纯输入·input + cached_tokens·cache_read + cache_creation_tokens·cache_creation + output_tokens·output) / 1e6
```

修改单价会按新价重算全部历史花费。

## 账号用量页面

插件注册了一个面板菜单"账号用量"，页面路径 `/v0/resource/plugins/usage-statistics/ui`。配套面板和官方面板都会把它显示在侧边栏，并以 iframe 嵌入。页面本身不需要鉴权且不含任何数据，数据都通过上面那些需要管理密钥的接口获取。

## 说明

- 本插件仅使用上游 `UsageRecord` ABI 现有字段。失败判定为 `Failed || failure_status_code >= 400`。
- 升级或回滚请走"停用插件 → 替换 `.so` → 启用插件"：停用时宿主会先调用插件的 shutdown，关闭数据库，然后才加载新版本，所以不需要重启 CLIProxyAPI。停用期间产生的用量记录会丢失。
- 不要在插件启用时直接替换 `.so` 并改配置（即热重载）。0.1.0 和 0.2.0-acct.1 在热重载时，新旧两份插件各带一份 SQLite 同时写同一个 `usage.db`，会丢记录并损坏数据库。从 0.3.0-acct.1 起，插件会在被替换前（`plugin.quiesce`）关闭数据库，热重载不再损坏数据库，但替换期间的记录仍会丢失。
