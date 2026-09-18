package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

// pendingUsage is a usage.handle record parked until a stream chunk or
// non-streaming response claims it. Records for the same request can arrive
// more than once (executors publish per usage event); the newest wins.
type pendingUsage struct {
	rec       pluginapi.UsageRecord
	arrivedAt time.Time
}

// requestCtx remembers per-request correlation data stashed at
// request.intercept_before / stream header-init time.
type requestCtx struct {
	requestID string
	model     string // normalized/upstream model
	requested string
	sourceFmt string
	stream    bool
	apiKey    string // inbound client key (may differ from hashed UsageRecord.APIKey)
	sessionID string
	startedAt time.Time

	startInput   int64 // usage.input_tokens carried by message_start
	deltaInput   int64 // input_tokens in the last message_delta carrying usage
	patched      bool
	injectRead   int64
	injectCreate int64
}

// cachePluginState is the whole plugin runtime state. All fields are guarded
// by mu; interceptor callbacks run on host threads and usage callbacks on the
// usage dispatcher thread.
type cachePluginState struct {
	mu       sync.Mutex
	requests map[string]*requestCtx
	pending  []pendingUsage // FIFO

	statRequests   int64
	statChunksSeen int64
	statUsageSeen  int64
	statUsageOrph  int64
	statPatched    int64
	statMissed     int64
	statNonStream  int64

	recentMu sync.Mutex
	recent   []recentInject
}

type recentInject struct {
	At       time.Time `json:"at"`
	Request  string    `json:"request_id"`
	Model    string    `json:"model"`
	Input    int64     `json:"input_tokens"`
	Read     int64     `json:"cache_read"`
	Create   int64     `json:"cache_create"`
	Stream   bool      `json:"stream"`
	WaitedMS int64     `json:"waited_ms"`
}

var state = &cachePluginState{
	requests: make(map[string]*requestCtx),
}

func (s *cachePluginState) flush() {}

// --- helpers ---------------------------------------------------------------

func headerGet(h http.Header, name string) string {
	if h == nil {
		return ""
	}
	return strings.TrimSpace(h.Get(name))
}

func apiKeyFromHeaders(h http.Header) string {
	for _, name := range []string{"x-api-key", "authorization"} {
		v := headerGet(h, name)
		if v == "" {
			continue
		}
		v = strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
		return v
	}
	return ""
}

// sessionIDFromRequest extracts a weak session signal. Correlation never
// depends on it alone — exact input-token equality does the real work.
func sessionIDFromRequest(body []byte, headers http.Header) string {
	if v := headerGet(headers, "session_id"); v != "" {
		return v
	}
	if v := headerGet(headers, "x-session-id"); v != "" {
		return v
	}
	var parsed struct {
		Metadata struct {
			UserID    string `json:"user_id"`
			SessionID string `json:"session_id"`
		} `json:"metadata"`
	}
	if json.Unmarshal(body, &parsed) == nil {
		if parsed.Metadata.SessionID != "" {
			return parsed.Metadata.SessionID
		}
		return parsed.Metadata.UserID
	}
	return ""
}

func num(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	}
	return 0
}

// --- lifecycle callbacks ----------------------------------------------------

func (s *cachePluginState) noteRequest(req *pluginapi.RequestInterceptRequest) {
	if req == nil || req.RequestID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rc := s.requests[req.RequestID]
	if rc == nil {
		rc = &requestCtx{requestID: req.RequestID, startedAt: time.Now()}
		s.requests[req.RequestID] = rc
		s.statRequests++
	}
	// enrich with whatever this call carries
	if req.Model != "" {
		rc.model = req.Model
	}
	if req.RequestedModel != "" {
		rc.requested = req.RequestedModel
	}
	if req.SourceFormat != "" {
		rc.sourceFmt = req.SourceFormat
	}
	rc.stream = req.Stream
	if k := apiKeyFromHeaders(req.Headers); k != "" {
		rc.apiKey = k
	}
	if sid := sessionIDFromRequest(req.Body, req.Headers); sid != "" {
		rc.sessionID = sid
	}
	s.pruneLocked()
}

func (s *cachePluginState) noteUsage(rec *pluginapi.UsageRecord) {
	if rec == nil {
		return
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statUsageSeen++
	// merge consecutive publishes for the same request signature: latest wins.
	for i := range s.pending {
		p := &s.pending[i]
		if p.rec.Model == rec.Model &&
			p.rec.APIKey == rec.APIKey &&
			p.rec.SessionID == rec.SessionID &&
			p.rec.AuthIndex == rec.AuthIndex &&
			p.rec.RequestedAt.Equal(rec.RequestedAt) {
			p.rec = *rec
			p.arrivedAt = now
			return
		}
	}
	s.pending = append(s.pending, pendingUsage{rec: *rec, arrivedAt: now})
	// opportunistic expiry
	cut := now.Add(-time.Duration((*settings.Load()).PendingTTLMS) * time.Millisecond)
	keep := s.pending[:0]
	orphan := 0
	for _, p := range s.pending {
		if p.arrivedAt.Before(cut) {
			orphan++
			continue
		}
		keep = append(keep, p)
	}
	s.pending = keep
	s.statUsageOrph += int64(orphan)
	s.pruneLocked()
}

func (s *cachePluginState) noteComplete(c *pluginapi.RequestCompletion) {
	if c == nil || c.RequestID == "" {
		return
	}
	s.mu.Lock()
	delete(s.requests, c.RequestID)
	s.mu.Unlock()
}

func (s *cachePluginState) pruneLocked() {
	if len(s.requests) > 4096 {
		cut := time.Now().Add(-10 * time.Minute)
		for id, rc := range s.requests {
			if rc.startedAt.Before(cut) {
				delete(s.requests, id)
			}
		}
	}
}

// --- usage record matching --------------------------------------------------

// claimUsage finds and removes the pending record best matching rc.
//
// Matching rules (deliberately conservative on identity, liberal on timing):
//   - model must equal rc.model when both sides are set
//   - session must equal when both sides are set
//   - apiKey is a score bonus only (UsageRecord.APIKey may be hashed)
//   - an exact InputTokens match is the dominant score term
//   - records with OutputTokens>0 (terminal) are preferred
func (s *cachePluginState) claimUsage(rc *requestCtx, wantInput int64) *pluginapi.UsageRecord {
	best := -1
	bestScore := int64(-1)
	now := time.Now()
	for i := range s.pending {
		p := &s.pending[i]
		if rc.model != "" && p.rec.Model != "" && p.rec.Model != rc.model {
			continue
		}
		if rc.sessionID != "" && p.rec.SessionID != "" && p.rec.SessionID != rc.sessionID {
			continue
		}
		var score int64
		if wantInput > 0 && p.rec.Detail.InputTokens == wantInput {
			score += 100
		} else if wantInput > 0 && p.rec.Detail.InputTokens > 0 {
			diff := p.rec.Detail.InputTokens - wantInput
			if diff < 0 {
				diff = -diff
			}
			if diff <= 64 {
				score += 60
			}
		}
		if rc.apiKey != "" && p.rec.APIKey != "" && p.rec.APIKey == rc.apiKey {
			score += 30
		}
		if p.rec.Detail.OutputTokens > 0 {
			score += 20
		}
		age := now.Sub(p.arrivedAt)
		switch {
		case age < 5*time.Second:
			score += 10
		case age < 30*time.Second:
			score += 5
		}
		if score > bestScore {
			bestScore = score
			best = i
		}
	}
	// Require at least a fresh record or an input match — avoids attaching a
	// completely unrelated usage record when nothing plausible exists.
	if best < 0 || bestScore < 15 {
		return nil
	}
	rec := s.pending[best].rec
	s.pending = append(s.pending[:best], s.pending[best+1:]...)
	return &rec
}

// waitUsage polls pending until a match shows up or the deadline passes.
// Called while a terminal usage chunk is being held — the delay directly
// stalls the client's last SSE frame, so keep the configured bound tight.
func (s *cachePluginState) waitUsage(rc *requestCtx, wantInput int64, cfg config) (*pluginapi.UsageRecord, int64) {
	deadline := time.Now().Add(time.Duration(cfg.UsageWaitMS) * time.Millisecond)
	poll := time.Duration(cfg.UsageWaitPollMS) * time.Millisecond
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	start := time.Now()
	for {
		s.mu.Lock()
		rec := s.claimUsage(rc, wantInput)
		s.mu.Unlock()
		if rec != nil {
			return rec, time.Since(start).Milliseconds()
		}
		if time.Now().After(deadline) {
			return nil, time.Since(start).Milliseconds()
		}
		time.Sleep(poll)
	}
}

// --- response patching -------------------------------------------------------

func (s *cachePluginState) enabledFor(sourceFmt string, cfg config) bool {
	if !cfg.Enabled {
		return false
	}
	if len(cfg.SourceFormats) == 0 {
		return true
	}
	for _, f := range cfg.SourceFormats {
		if strings.EqualFold(f, sourceFmt) {
			return true
		}
	}
	return false
}

// patchNonStream handles response.intercept_after (non-streaming).
func (s *cachePluginState) patchNonStream(req *pluginapi.ResponseInterceptRequest, cfg config) []byte {
	if !s.enabledFor(req.SourceFormat, cfg) || len(req.Body) == 0 {
		return nil
	}
	s.mu.Lock()
	s.statNonStream++
	s.mu.Unlock()
	var body map[string]any
	if json.Unmarshal(req.Body, &body) != nil {
		return nil
	}
	usageObj, _ := body["usage"].(map[string]any)
	if usageObj == nil {
		return nil
	}
	if num(usageObj["cache_read_input_tokens"]) > 0 || num(usageObj["cache_creation_input_tokens"]) > 0 {
		return nil // already carries cache fields
	}
	rc := s.lookupRequest(req.RequestID)
	if rc == nil {
		rc = &requestCtx{
			requestID: req.RequestID,
			model:     req.Model,
			requested: req.RequestedModel,
			sourceFmt: req.SourceFormat,
			apiKey:    apiKeyFromHeaders(req.RequestHeaders),
			sessionID: sessionIDFromRequest(req.OriginalRequest, req.RequestHeaders),
		}
	}
	in := num(usageObj["input_tokens"])
	rec, waited := s.waitUsage(rc, in, cfg)
	if rec == nil {
		s.miss(req.RequestID)
		return nil
	}
	patchUsageObject(usageObj, rec, cfg)
	out, err := json.Marshal(body)
	if err != nil {
		return nil
	}
	s.hit(req.RequestID, rc, rec, false, waited)
	return out
}

// patchStreamChunk handles response.intercept_stream_chunk.
func (s *cachePluginState) patchStreamChunk(req *pluginapi.StreamChunkInterceptRequest, cfg config) []byte {
	if req.ChunkIndex == pluginapi.StreamChunkHeaderInitIndex {
		s.noteRequest(&pluginapi.RequestInterceptRequest{
			RequestID:      req.RequestID,
			SourceFormat:   req.SourceFormat,
			Model:          req.Model,
			RequestedModel: req.RequestedModel,
			Stream:         true,
			Headers:        req.RequestHeaders,
			Body:           req.OriginalRequest,
		})
		return nil
	}
	if !s.enabledFor(req.SourceFormat, cfg) || len(req.Body) == 0 {
		return nil
	}
	s.mu.Lock()
	s.statChunksSeen++
	s.mu.Unlock()
	out, _ := s.patchSSEFrame(req, cfg)
	return out
}

// patchSSEFrame rewrites one SSE frame's data payload.
// Frame layout: "event: X\ndata: {json}\n\n".
func (s *cachePluginState) patchSSEFrame(req *pluginapi.StreamChunkInterceptRequest, cfg config) ([]byte, bool) {
	body := req.Body
	dataStart := -1
	for i := 0; i+5 <= len(body); i++ {
		if body[i] == 'd' && string(body[i:i+5]) == "data:" {
			dataStart = i + 5
			for dataStart < len(body) && body[dataStart] == ' ' {
				dataStart++
			}
			break
		}
	}
	if dataStart < 0 {
		return nil, false
	}
	dataEnd := dataStart
	for dataEnd < len(body) && body[dataEnd] != '\n' && body[dataEnd] != '\r' {
		dataEnd++
	}
	payload := body[dataStart:dataEnd]
	var ev map[string]any
	if json.Unmarshal(payload, &ev) != nil {
		return nil, false
	}
	evType, _ := ev["type"].(string)

	rc := s.lookupRequest(req.RequestID)
	if rc == nil {
		rc = &requestCtx{
			requestID: req.RequestID,
			model:     req.Model,
			requested: req.RequestedModel,
			sourceFmt: req.SourceFormat,
			stream:    true,
			apiKey:    apiKeyFromHeaders(req.RequestHeaders),
			sessionID: sessionIDFromRequest(req.OriginalRequest, req.RequestHeaders),
		}
	}

	switch evType {
	case "message_start":
		msg, _ := ev["message"].(map[string]any)
		usageObj, _ := msg["usage"].(map[string]any)
		if usageObj == nil {
			return nil, false
		}
		s.mu.Lock()
		rc.startInput = num(usageObj["input_tokens"])
		s.mu.Unlock()
		// Emit Anthropic schema fields with zero values so downstream parsers
		// (e.g. sub2api's message_start handler) see a complete usage object.
		// Real numbers arrive on the terminal message_delta.
		if _, has := usageObj["cache_read_input_tokens"]; !has {
			usageObj["cache_read_input_tokens"] = 0
			usageObj["cache_creation_input_tokens"] = 0
			usageObj["cache_creation"] = map[string]any{
				"ephemeral_5m_input_tokens": 0,
				"ephemeral_1h_input_tokens": 0,
			}
			return replaceData(body, dataStart, dataEnd, ev), true
		}
		return nil, false

	case "message_delta":
		usageObj, _ := ev["usage"].(map[string]any)
		if usageObj == nil {
			return nil, false
		}
		if num(usageObj["cache_read_input_tokens"]) > 0 || num(usageObj["cache_creation_input_tokens"]) > 0 {
			return nil, false
		}
		deltaInput := num(usageObj["input_tokens"])
		s.mu.Lock()
		rc.deltaInput = deltaInput
		s.mu.Unlock()
		want := deltaInput
		if want <= 0 {
			want = rc.startInput
		}
		rec, waited := s.waitUsage(rc, want, cfg)
		if rec == nil {
			s.miss(req.RequestID)
			return nil, false
		}
		patchUsageObject(usageObj, rec, cfg)
		out := replaceData(body, dataStart, dataEnd, ev)
		s.hit(req.RequestID, rc, rec, true, waited)
		return out, true
	}
	return nil, false
}

// patchUsageObject rewrites usage fields in place from a usage record.
// rec.Detail.InputTokens is the total input (cached included). With
// split-input=uncached, input_tokens becomes the uncached remainder so
// downstream billing charges cached tokens at cache-read price.
func patchUsageObject(usageObj map[string]any, rec *pluginapi.UsageRecord, cfg config) {
	read := rec.Detail.CacheReadTokens
	create := rec.Detail.CacheCreationTokens
	if read == 0 && rec.Detail.CachedTokens > 0 {
		read = rec.Detail.CachedTokens
	}
	totalIn := rec.Detail.InputTokens
	if totalIn <= 0 {
		totalIn = num(usageObj["input_tokens"])
	}
	uncached := totalIn - read - create
	if uncached < 0 {
		uncached = 0
	}
	usageObj["cache_read_input_tokens"] = read
	usageObj["cache_creation_input_tokens"] = create
	if _, has := usageObj["cache_creation"]; !has || create > 0 {
		usageObj["cache_creation"] = map[string]any{
			"ephemeral_5m_input_tokens": create,
			"ephemeral_1h_input_tokens": 0,
		}
	}
	if strings.EqualFold(cfg.SplitInput, "uncached") && (read > 0 || create > 0) {
		usageObj["input_tokens"] = uncached
	}
}

func replaceData(body []byte, start, end int, ev map[string]any) []byte {
	raw, err := json.Marshal(ev)
	if err != nil {
		return body
	}
	out := make([]byte, 0, len(body)-(end-start)+len(raw))
	out = append(out, body[:start]...)
	out = append(out, raw...)
	out = append(out, body[end:]...)
	return out
}

func (s *cachePluginState) lookupRequest(id string) *requestCtx {
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[id]
}

func (s *cachePluginState) hit(requestID string, rc *requestCtx, rec *pluginapi.UsageRecord, stream bool, waited int64) {
	s.mu.Lock()
	s.statPatched++
	rc.patched = true
	rc.injectRead = rec.Detail.CacheReadTokens
	rc.injectCreate = rec.Detail.CacheCreationTokens
	s.mu.Unlock()
	if (*settings.Load()).LogInjections {
		fmt.Fprintf(os.Stderr, "[%s] injected request=%s model=%s input=%d read=%d create=%d stream=%v waited=%dms\n",
			pluginName, requestID, rec.Model, rec.Detail.InputTokens,
			rec.Detail.CacheReadTokens, rec.Detail.CacheCreationTokens, stream, waited)
	}
	s.recentMu.Lock()
	s.recent = append(s.recent, recentInject{
		At: time.Now(), Request: requestID, Model: rec.Model,
		Input: rec.Detail.InputTokens, Read: rec.Detail.CacheReadTokens,
		Create: rec.Detail.CacheCreationTokens, Stream: stream, WaitedMS: waited,
	})
	if len(s.recent) > 200 {
		s.recent = s.recent[len(s.recent)-200:]
	}
	s.recentMu.Unlock()
}

func (s *cachePluginState) miss(requestID string) {
	s.mu.Lock()
	s.statMissed++
	s.mu.Unlock()
	if (*settings.Load()).LogMisses {
		fmt.Fprintf(os.Stderr, "[%s] miss request=%s (no matching usage record)\n", pluginName, requestID)
	}
}

// --- management status route --------------------------------------------------

func managementRegistration() map[string]any {
	return map[string]any{
		"routes": []map[string]any{
			{"method": "GET", "path": "cache-usage/status", "menu": "Cache Usage", "description": "cache-usage plugin injection status"},
		},
		"resources": []map[string]any{
			{"path": "status", "menu": "Cache Usage", "description": "cache-usage plugin injection status"},
		},
	}
}

func (s *cachePluginState) handleManagement(req *pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	s.mu.Lock()
	stats := map[string]any{
		"requests_tracked": s.statRequests,
		"chunks_seen":      s.statChunksSeen,
		"usage_seen":       s.statUsageSeen,
		"usage_orphaned":   s.statUsageOrph,
		"patched":          s.statPatched,
		"missed":           s.statMissed,
		"non_stream":       s.statNonStream,
		"pending_usage":    len(s.pending),
		"open_requests":    len(s.requests),
	}
	s.mu.Unlock()
	s.recentMu.Lock()
	recent := append([]recentInject(nil), s.recent...)
	s.recentMu.Unlock()
	body, _ := json.Marshal(map[string]any{
		"plugin": pluginName, "version": pluginVersion,
		"stats": stats, "recent": recent,
		"config": *settings.Load(),
	})
	return pluginapi.ManagementResponse{
		StatusCode: 200,
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       body,
	}
}
