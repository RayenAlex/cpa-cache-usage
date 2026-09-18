package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func testConfig() config {
	c := defaultConfig()
	c.UsageWaitMS = 300
	c.UsageWaitPollMS = 10
	return c
}

func resetState() {
	state.mu.Lock()
	state.requests = make(map[string]*requestCtx)
	state.pending = nil
	state.mu.Unlock()
}

func mkDeltaFrame(input, output int64) []byte {
	ev := map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": "end_turn"},
		"usage": map[string]any{"input_tokens": input, "output_tokens": output},
	}
	raw, _ := json.Marshal(ev)
	return []byte("event: message_delta\ndata: " + string(raw) + "\n\n")
}

func mkStartFrame(input int64) []byte {
	ev := map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": "msg_1", "type": "message", "role": "assistant",
			"model": "devin/swe-2",
			"usage": map[string]any{"input_tokens": input, "output_tokens": 0},
		},
	}
	raw, _ := json.Marshal(ev)
	return []byte("event: message_start\ndata: " + string(raw) + "\n\n")
}

func usageRec(model string, in, out, read, create int64) pluginapi.UsageRecord {
	return pluginapi.UsageRecord{
		Provider: "devin", Model: model, APIKey: "sk-cpa-test",
		RequestedAt: time.Now(),
		Detail: pluginapi.UsageDetail{
			InputTokens: in, OutputTokens: out,
			CacheReadTokens: read, CacheCreationTokens: create,
			TotalTokens: in + out,
		},
	}
}

func TestMessageDeltaInjection(t *testing.T) {
	resetState()
	cfg := testConfig()
	state.noteRequest(&pluginapi.RequestInterceptRequest{
		RequestID: "r1", SourceFormat: "claude", Model: "devin/swe-2", Stream: true,
		Headers: http.Header{"X-Api-Key": []string{"sk-cpa-test"}},
	})
	state.noteUsage(usageRecPtr("devin/swe-2", 74696, 302, 74048, 0))

	frame := mkDeltaFrame(74696, 302)
	out := state.patchStreamChunk(&pluginapi.StreamChunkInterceptRequest{
		RequestID: "r1", SourceFormat: "claude", Model: "devin/swe-2",
		Body: frame, ChunkIndex: 3,
	}, cfg)
	if out == nil {
		t.Fatal("expected patched frame")
	}
	var ev map[string]any
	json.Unmarshal(extractData(out), &ev)
	u := ev["usage"].(map[string]any)
	if got := num(u["cache_read_input_tokens"]); got != 74048 {
		t.Fatalf("cache_read_input_tokens=%d want 74048", got)
	}
	// split-input=uncached: 74696-74048-0 = 648
	if got := num(u["input_tokens"]); got != 648 {
		t.Fatalf("input_tokens=%d want 648", got)
	}
	if got := num(u["output_tokens"]); got != 302 {
		t.Fatalf("output_tokens=%d want 302", got)
	}
}

func TestMessageStartZeroFields(t *testing.T) {
	resetState()
	cfg := testConfig()
	frame := mkStartFrame(9)
	out := state.patchStreamChunk(&pluginapi.StreamChunkInterceptRequest{
		RequestID: "r2", SourceFormat: "claude", Model: "devin/swe-2",
		Body: frame, ChunkIndex: 0,
	}, cfg)
	if out == nil {
		t.Fatal("expected patched message_start")
	}
	var ev map[string]any
	json.Unmarshal(extractData(out), &ev)
	u := ev["message"].(map[string]any)["usage"].(map[string]any)
	if _, ok := u["cache_read_input_tokens"]; !ok {
		t.Fatal("missing cache_read_input_tokens")
	}
	if num(u["input_tokens"]) != 9 {
		t.Fatal("input_tokens should be unchanged on message_start")
	}
}

func TestNonStreamInjection(t *testing.T) {
	resetState()
	cfg := testConfig()
	state.noteUsage(usageRecPtr("devin/swe-2", 471, 16, 460, 0))
	respBody := []byte(`{"id":"x","type":"message","model":"devin/swe-2","usage":{"input_tokens":471,"output_tokens":16}}`)
	out := state.patchNonStream(&pluginapi.ResponseInterceptRequest{
		RequestID: "r3", SourceFormat: "claude", Model: "devin/swe-2",
		Body: respBody, StatusCode: 200,
	}, cfg)
	if out == nil {
		t.Fatal("expected patched body")
	}
	var body map[string]any
	json.Unmarshal(out, &body)
	u := body["usage"].(map[string]any)
	if num(u["cache_read_input_tokens"]) != 460 {
		t.Fatalf("cache_read=%v", u["cache_read_input_tokens"])
	}
	if num(u["input_tokens"]) != 11 {
		t.Fatalf("input_tokens=%v want 11", u["input_tokens"])
	}
}

func TestMissPassthrough(t *testing.T) {
	resetState()
	cfg := testConfig()
	cfg.UsageWaitMS = 50
	frame := mkDeltaFrame(100, 5)
	out := state.patchStreamChunk(&pluginapi.StreamChunkInterceptRequest{
		RequestID: "r4", SourceFormat: "claude", Model: "devin/swe-2",
		Body: frame, ChunkIndex: 3,
	}, cfg)
	if out != nil {
		t.Fatal("expected nil (passthrough) on miss")
	}
}

func TestOtherFormatSkipped(t *testing.T) {
	resetState()
	cfg := testConfig()
	state.noteUsage(usageRecPtr("x", 100, 5, 90, 0))
	out := state.patchNonStream(&pluginapi.ResponseInterceptRequest{
		RequestID: "r5", SourceFormat: "openai", Model: "x",
		Body: []byte(`{"usage":{"input_tokens":100,"output_tokens":5}}`),
	}, cfg)
	if out != nil {
		t.Fatal("openai format should be skipped")
	}
}

func TestAlreadyHasCacheFields(t *testing.T) {
	resetState()
	cfg := testConfig()
	frame := []byte(`event: message_delta
data: {"type":"message_delta","usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":8}}

`)
	out := state.patchStreamChunk(&pluginapi.StreamChunkInterceptRequest{
		RequestID: "r6", SourceFormat: "claude", Model: "m",
		Body: frame, ChunkIndex: 3,
	}, cfg)
	if out != nil {
		t.Fatal("should pass through when cache fields already present")
	}
}

func extractData(frame []byte) []byte {
	s := string(frame)
	i := 0
	for {
		j := indexOf(s[i:], "data:")
		if j < 0 {
			return nil
		}
		i += j + 5
		for i < len(s) && s[i] == ' ' {
			i++
		}
		end := i
		for end < len(s) && s[end] != '\n' && s[end] != '\r' {
			end++
		}
		return []byte(s[i:end])
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func usageRecPtr(model string, in, out, read, create int64) *pluginapi.UsageRecord {
	r := usageRec(model, in, out, read, create)
	return &r
}
