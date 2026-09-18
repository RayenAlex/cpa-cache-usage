package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const pluginName = "cpa-cache-usage"
const pluginVersion = "0.1.1"

// config controls which responses get cache-usage injection and how the plugin
// correlates intercepted chunks with usage.handle records.
type config struct {
	Enabled bool `yaml:"enabled"`
	// SourceFormats limits injection to these inbound protocol formats.
	// "claude" is the /v1/messages format that drops usage.cache_* fields.
	SourceFormats []string `yaml:"source-formats"`
	// UsageWaitMS bounds how long the interceptor waits for a usage.handle
	// record on the terminal usage chunk before passing it through unchanged.
	UsageWaitMS int `yaml:"usage-wait-ms"`
	// UsageWaitPollMS is the poll interval while waiting for usage records.
	UsageWaitPollMS int `yaml:"usage-wait-poll-ms"`
	// SplitInput controls usage.input_tokens semantics after injection.
	// "keep" leaves CPA's total (cached + uncached) input_tokens untouched —
	// cache fields are additive metadata only.
	// "uncached" rewrites input_tokens to uncached input
	// (total - cache_read - cache_creation), matching Anthropic semantics so
	// downstream gateways bill cached tokens at cache-read price.
	SplitInput string `yaml:"split-input"`
	// PendingTTLMS is how long usage.handle records stay matchable.
	PendingTTLMS int `yaml:"pending-ttl-ms"`
	// LogMisses logs a line to stderr when a terminal usage chunk could not
	// be matched to a usage record.
	LogMisses bool `yaml:"log-misses"`
	// LogInjections logs every successful injection.
	LogInjections bool `yaml:"log-injections"`
}

func defaultConfig() config {
	return config{
		Enabled:         true,
		SourceFormats:   []string{"claude"},
		UsageWaitMS:     900,
		UsageWaitPollMS: 50,
		SplitInput:      "uncached",
		PendingTTLMS:    300_000,
		LogMisses:       true,
		LogInjections:   false,
	}
}

var settings atomic.Pointer[config]

func init() {
	c := defaultConfig()
	settings.Store(&c)
}

func main() { fmt.Println(pluginName, pluginVersion) }

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		var request struct {
			ConfigYAML []byte `json:"config_yaml"`
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &request); err != nil {
				return nil, fmt.Errorf("invalid lifecycle request")
			}
		}
		c := defaultConfig()
		if err := yaml.Unmarshal(request.ConfigYAML, &c); err != nil {
			return nil, fmt.Errorf("invalid plugin configuration")
		}
		settings.Store(&c)
		return okEnvelope(struct {
			SchemaVersion uint32             `json:"schema_version"`
			Metadata      pluginapi.Metadata `json:"metadata"`
			Capabilities  map[string]bool    `json:"capabilities"`
		}{pluginabi.SchemaVersion, pluginapi.Metadata{
			Name: pluginName, Version: pluginVersion, Author: "rayen",
			// CPA 要求填写该字段。这里指向本插件仓库。
			GitHubRepository: "https://github.com/rayenalex/cpa-cache-usage",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "是否启用 /v1/messages 响应缓存用量注入。"},
				{Name: "source-formats", Type: pluginapi.ConfigFieldTypeArray, Description: "只对列出的入站协议格式注入，默认 [claude]（/v1/messages）。"},
				{Name: "usage-wait-ms", Type: pluginapi.ConfigFieldTypeInteger, Description: "终止 usage chunk 等待 usage.handle 记录的最大毫秒数。"},
				{Name: "usage-wait-poll-ms", Type: pluginapi.ConfigFieldTypeInteger, Description: "等待 usage 记录的轮询间隔毫秒数。"},
				{Name: "split-input", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"keep", "uncached"}, Description: "keep: 保留 CPA 的 input_tokens 总量只加缓存字段；uncached: 把 input_tokens 改成未缓存部分（Anthropic 语义，避免缓存部分被按全价计费）。"},
				{Name: "pending-ttl-ms", Type: pluginapi.ConfigFieldTypeInteger, Description: "usage.handle 记录可被匹配的有效期毫秒数。"},
				{Name: "log-misses", Type: pluginapi.ConfigFieldTypeBoolean, Description: "无法匹配 usage 记录时输出一行日志。"},
				{Name: "log-injections", Type: pluginapi.ConfigFieldTypeBoolean, Description: "每次成功注入输出一行日志。"},
			},
		}, map[string]bool{
			"request_interceptor":         true,
			"response_interceptor":        true,
			"response_stream_interceptor": true,
			"request_lifecycle_plugin":    true,
			"usage_plugin":                true,
			"management_api":              true,
		}})
	case pluginabi.MethodRequestInterceptBefore:
		var req pluginapi.RequestInterceptRequest
		if json.Unmarshal(raw, &req) != nil {
			return okEnvelope(pluginapi.RequestInterceptResponse{})
		}
		state.noteRequest(&req)
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	case pluginabi.MethodUsageHandle:
		var rec pluginapi.UsageRecord
		if json.Unmarshal(raw, &rec) != nil {
			return okEnvelope(struct{}{})
		}
		state.noteUsage(&rec)
		return okEnvelope(struct{}{})
	case pluginabi.MethodResponseInterceptAfter:
		var req pluginapi.ResponseInterceptRequest
		if json.Unmarshal(raw, &req) != nil {
			return okEnvelope(pluginapi.ResponseInterceptResponse{})
		}
		out := state.patchNonStream(&req, *settings.Load())
		if out == nil {
			return okEnvelope(pluginapi.ResponseInterceptResponse{})
		}
		return okEnvelope(pluginapi.ResponseInterceptResponse{Body: out})
	case pluginabi.MethodResponseInterceptStreamChunk:
		var req pluginapi.StreamChunkInterceptRequest
		if json.Unmarshal(raw, &req) != nil {
			return okEnvelope(pluginapi.StreamChunkInterceptResponse{})
		}
		out := state.patchStreamChunk(&req, *settings.Load())
		if out == nil {
			return okEnvelope(pluginapi.StreamChunkInterceptResponse{})
		}
		return okEnvelope(pluginapi.StreamChunkInterceptResponse{Body: out})
	case pluginabi.MethodRequestComplete:
		var completion pluginapi.RequestCompletion
		if json.Unmarshal(raw, &completion) != nil {
			return okEnvelope(struct{}{})
		}
		state.noteComplete(&completion)
		return okEnvelope(struct{}{})
	case pluginabi.MethodManagementRegister:
		return okEnvelope(managementRegistration())
	case pluginabi.MethodManagementHandle:
		var req pluginapi.ManagementRequest
		if json.Unmarshal(raw, &req) != nil {
			return okEnvelope(pluginapi.ManagementResponse{StatusCode: 400})
		}
		return okEnvelope(state.handleManagement(&req))
	case pluginabi.MethodRequestInterceptAfter, pluginabi.MethodPluginQuiesce, pluginabi.MethodPluginShutdown:
		return okEnvelope(pluginapi.RequestInterceptResponse{})
	default:
		return errorEnvelope("unknown_method", "unsupported method"), nil
	}
}

func okEnvelope(result any) ([]byte, error) {
	return json.Marshal(struct {
		OK     bool `json:"ok"`
		Result any  `json:"result"`
	}{true, result})
}

func errorEnvelope(code, message string) []byte {
	out, _ := json.Marshal(map[string]any{"ok": false, "error": map[string]string{"code": code, "message": message}})
	return out
}
