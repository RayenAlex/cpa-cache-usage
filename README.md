# cpa-cache-usage

CPA (CLIProxyAPI) plugin that injects real prompt-cache usage into `/v1/messages`
(Anthropic-format) responses, which the upstream CPA binary serializes without
`cache_read_input_tokens` / `cache_creation_input_tokens` even when the cache
was hit — so downstream gateways (e.g. sub2api) see a 0% cache-hit rate and
bill cached input at full price.

## How it works

CPA's own usage pipeline *does* record cache tokens (`usage_events` shows
`cache_read_tokens` ≈ 95% of input for `devin/swe-2` traffic), the executor
just drops the fields when serializing `/v1/messages` responses.

The plugin combines two hooks:

- **`usage.handle`** — receives the real `UsageDetail`
  (`CacheReadTokens`/`CacheCreationTokens`) from the host's usage manager.
  Executors publish this mid-stream, *before* the terminal SSE chunk reaches
  the client.
- **`response.intercept_stream_chunk`** — when a `message_delta` chunk with
  `usage` arrives without cache fields, the plugin briefly waits (default
  ≤900ms, usually instant) for the matching `usage.handle` record, injects the
  cache counts, and rewrites `input_tokens` to the uncached remainder.
- **`message_start`** gets `cache_read_input_tokens`/`cache_creation_input_tokens`/
  `cache_creation` zero-fields so downstream parsers see the full Anthropic
  usage schema.
- **`response.intercept_after`** patches non-streaming `/v1/messages` bodies
  the same way.
- **`request.intercept_before` / `request.complete`** maintain the per-request
  correlation state (`RequestID`, model, api key, session).

Records are matched to requests by model + exact `input_tokens` + session +
api key + recency; ambiguous matches are refused rather than misattributed.

### input_tokens semantics

CPA reports `usage.input_tokens` as the *total* input (cached included).
With `split-input: uncached` (default) the plugin rewrites it to
`total - cache_read - cache_creation`, matching Anthropic API semantics — so
downstream billing (sub2api) charges cached tokens at cache-read price.
Set `split-input: keep` to only add the cache fields without touching
`input_tokens`.

## Install

Requires CPA built with the plugin host (seakee fork / CLIProxyAPI ≥ v7.2.x
schema 6). Linux amd64/arm64 only (Go c-shared plugin).

1. Download `cpa-cache-usage_<ver>_linux_<arch>.zip` from releases, or build:
   `./scripts/package-release.sh`
2. Via the CPA plugin store: add this repo's registry entry to
   `plugins.store-sources`, then install `cpa-cache-usage` from the store UI.
   Or drop `cpa-cache-usage.so` into the CPA `plugins/` directory and add:

```yaml
plugins:
  enabled: true
  configs:
    cpa-cache-usage:
      enabled: true
      split-input: uncached      # keep | uncached
      usage-wait-ms: 900         # max wait on terminal usage chunk
      log-misses: true
      log-injections: false
```

3. Restart / reload plugins.

## Verify

The plugin exposes a status route at:

```
GET /v0/management/cache-usage/status
GET /v0/resource/plugins/cpa-cache-usage/status
```

`stats.chunks_seen` > 0 confirms the stream interceptor is active;
`stats.patched` counts injected responses; `recent` lists the last 200
injections with per-request wait times.

After a few requests, check the downstream gateway: the `cpa` account's
`cache_read_tokens` should start rising and `input_tokens` should drop to
the uncached remainder.

## Config reference

| key | default | meaning |
|---|---|---|
| `enabled` | `true` | master switch |
| `source-formats` | `[claude]` | inbound protocol formats to patch |
| `usage-wait-ms` | `900` | max wait for the usage record on the terminal chunk |
| `usage-wait-poll-ms` | `50` | poll interval while waiting |
| `split-input` | `uncached` | `keep` or `uncached` |
| `pending-ttl-ms` | `300000` | expiry for unmatched usage records |
| `log-misses` | `true` | stderr line when a terminal chunk can't be matched |
| `log-injections` | `false` | stderr line per successful injection |

## Known limits

- The `usage.handle` record is published when the executor sees the usage
  line — for all observed executors that precedes the terminal chunk, so the
  wait almost never stalls. If a future executor publishes only at stream
  close, misses increase (`usage-wait-ms` tunes the worst-case added latency).
- Correlation is heuristic. Under identical concurrent requests
  (same model+input+session), a wrong-but-numerically-identical record could
  attach — the numbers come from the same upstream accounting either way.
- The plugin does not fabricate counts: unmatched chunks pass through
  unchanged, and `missed` is counted on the status route.
