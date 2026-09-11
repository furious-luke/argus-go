package argus

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// NotifyEvent is a change notification pushed over a stream's notify WebSocket.
// It carries the frame that tripped change detection (or the initial frame
// delivered on subscribe).
type NotifyEvent struct {
	// StreamID is the UUID of the stream the event concerns.
	StreamID string
	// Track is the logical track that changed: TrackCamera or TrackScreen.
	Track TrackType
	// SSIMScore is the structural-similarity score against the previous baseline
	// frame; lower means a larger change. It is zero for the initial on-subscribe
	// frame.
	SSIMScore float64
	// Timestamp is when the change was detected, if provided.
	Timestamp time.Time
	// FrameFormat is the encoding of Frame (currently always "jpeg").
	FrameFormat string
	// Frame is the decoded image bytes.
	Frame []byte
}

// NotifyOptions configures a Subscribe call. Zero-valued fields use the server
// default.
type NotifyOptions struct {
	// Track selects which track to watch: TrackCamera or TrackScreen (default
	// TrackCamera).
	Track TrackType
	// Threshold is the change-detection threshold in (0,1].
	Threshold float64
	// PollIntervalMs is the watcher poll interval in milliseconds (>0).
	PollIntervalMs int
}

// NotifyHandlers receive events over a subscription's lifetime. All are optional.
//
// The transcription handlers fire when the stream publishes a microphone track.
// They arrive on the same subscription as frames; a transcript-only consumer
// (e.g. a voice agent that does not need video) simply leaves OnFrame nil and
// omits the frame watch options.
type NotifyHandlers struct {
	// OnFrame is called for each frame (initial and subsequent changes).
	OnFrame func(NotifyEvent)
	// OnSpeechStarted fires when the speaker begins talking. A voice agent uses
	// it to stop talking and yield the turn.
	OnSpeechStarted func()
	// OnTranscript fires with a complete utterance. Only final transcripts are
	// delivered — interim/partial text is never sent. transcriptionID is the
	// transcript's per-stream id, matching the transcription_timing diagnostics
	// trace for the same transcript.
	OnTranscript func(text string, transcriptionID uint64)
	// OnNoSpeech fires when an utterance produced no usable text (silence, noise,
	// unintelligible audio), so a voice agent can resume speaking.
	OnNoSpeech func()
	// OnTranscriptionInterrupted fires on a RECOVERABLE transcription break: the
	// active provider dropped/recovered, or active microphone input ended without
	// completing its endpoint. The consumer should ask the speaker to repeat when
	// transcription input is available again.
	OnTranscriptionInterrupted func()
	// OnTranscriptionUnavailable fires on a TERMINAL transcription break: every
	// provider failed and transcription will not resume for this stream. Frames
	// (if any) are unaffected.
	OnTranscriptionUnavailable func()
	// OnUtterance receives state changes for assistant speech submitted on this
	// same bidirectional socket.
	OnUtterance func(UtteranceEvent)
	// OnUserText receives valid typed browser input after Argus has cancelled any
	// speech it interrupted.
	OnUserText func(messageID, text string)
	// OnTokenExpiring fires when the control token is near expiry. The current client
	// does not support replacing the token in place; the subscription ends when
	// the gateway drops the expired connection.
	OnTokenExpiring func()
	// OnEnded fires when the stream ended or this subscription was superseded by a
	// newer one (e.g. the browser reconnected to a different node). After it
	// fires, Subscribe returns.
	OnEnded func(reason string)

	// Realtime-agent handlers (only fire on a realtime-mode stream). OnAgentEngaged
	// fires when the agent accepts a configuration and begins responding.
	// OnAgentError fires when configuration or the provider connection fails
	// (reason is a stable machine-readable code). OnAgentTranscript receives the
	// agent's OWN spoken-turn transcript; the user's speech continues to arrive on
	// OnTranscript. Both feed a customer's stored transcript when the agent, rather
	// than the customer server, produces the response.
	OnAgentEngaged    func()
	OnAgentError      func(reason string)
	OnAgentTranscript func(text string)
	// OnAgentToolCall fires when the agent calls a declared tool. The customer server
	// runs the function and returns the result with NotifySubscription.SubmitToolResult
	// (or SubmitToolError), correlating by AgentToolCall.CallID. If unset, tool calls
	// go unanswered and time out on the server.
	OnAgentToolCall func(call AgentToolCall)
	// OnAgentToolFailure reports that a previously delivered call can no longer
	// accept a result, carrying its call id and stable timeout/session-loss reason.
	// It may run concurrently with OnAgentToolCall when the call fails before that
	// handler returns.
	OnAgentToolFailure func(failure AgentToolFailure)
	// OnWorkerPushRejection reports that a PushWorker update was not accepted by the
	// agent (for example, a concurrent kind at its per-stream cap), so its content was
	// not delivered. The worker should hold the unit and retry rather than assume it
	// landed. If unset, rejections are dropped.
	OnWorkerPushRejection func(rejection WorkerPushRejection)
}

type UtteranceEvent struct {
	Type         string
	UtteranceID  string
	Reason       string
	DeliveryMode string
	TextComplete bool
}

// ApplicationTurnTiming describes where a customer application spent time
// between receiving a final transcript and returning the correlated reply.
// Every field is a duration measured on the application's monotonic clock.
type ApplicationTurnTiming struct {
	// Dispatch is populated automatically by StartUtteranceForTranscript from the
	// transcript callback to the command write. A supplied value is used only
	// when the client did not observe that transcript (for example after a handoff).
	Dispatch        time.Duration
	ModelQueue      time.Duration
	ModelTTFT       time.Duration
	ModelGeneration time.Duration
	ModelTotal      time.Duration
	ResponseBuffer  time.Duration
	Total           time.Duration
}

// Stable terminal-reason values carried by a NotifyTerminalError. They mirror
// the strings the Argus gateway emits on the wire, so integrations can classify
// a terminal error — e.g. a credential refresh versus a transport redial —
// against a named constant instead of an inline literal. Compare a reason
// against these rather than hard-coding the string; the wire values are pinned
// by TestSpec_NotifyReasons_PinWireValues on both sides of the boundary.
const (
	// NotifyReasonControlTokenExpired means the subscription's control token
	// expired. Refresh the control token and reconnect.
	NotifyReasonControlTokenExpired = "control token expired"
	// NotifyReasonReadTokenExpired means the subscription's read token expired.
	NotifyReasonReadTokenExpired = "read token expired"
	// NotifyReasonMeshConnectionLost means an established internal connection to
	// the owning media node dropped; redial.
	NotifyReasonMeshConnectionLost = "mesh connection lost"
	// NotifyReasonMeshConnectionUnavailable means the internal connection to the
	// owning media node could not be established; redial.
	NotifyReasonMeshConnectionUnavailable = "mesh connection unavailable"
	// NotifyReasonGatewayShuttingDown means the regional gateway is draining;
	// redial to land on another node.
	NotifyReasonGatewayShuttingDown = "gateway shutting down"
	// NotifyReasonStreamCommandQueueSaturated means the stream's command queue is
	// overloaded; redial.
	NotifyReasonStreamCommandQueueSaturated = "stream command queue saturated"
	// NotifyReasonStreamNotLive means the target stream has no live media session.
	NotifyReasonStreamNotLive = "stream not live"
	// NotifyReasonSubscribeFailed means the subscribe attempt failed on the media
	// node for a non-specific reason.
	NotifyReasonSubscribeFailed = "subscribe failed"
)

// NotifyTerminalError is a terminal error message sent by the regional gateway.
// Reason is a stable, machine-readable gateway reason; classify it against the
// NotifyReason* constants (not by parsing Error's presentation text) to tell,
// for example, a credential refresh from a transport redial.
type NotifyTerminalError struct {
	Reason string
}

func (e *NotifyTerminalError) Error() string {
	return fmt.Sprintf("notify error: %s", e.Reason)
}

// NotifyTerminalReason returns the gateway's machine-readable terminal reason.
// It lets integrations classify errors without depending on this concrete type.
func (e *NotifyTerminalError) NotifyTerminalReason() string { return e.Reason }

type NotifySubscription struct {
	conn      *websocket.Conn
	ctx       context.Context
	toolCalls *toolCallDispatcher
	writeMu   sync.Mutex
	done      chan struct{}
	errMu     sync.Mutex
	err       error
	closeOnce sync.Once
	turnMu    sync.Mutex
	// transcriptAnchors measures callback-to-reply dispatch on this process's
	// monotonic clock. It is bounded to recent interactive turns.
	transcriptAnchors map[uint64]time.Time
	transcriptOrder   []uint64
	// afterWriteCancel is a test-only completion barrier for the cancellation
	// callback installed around a blocked write. Nil in production.
	afterWriteCancel func()
}

const notifyWriteTimeout = time.Second
const notifyTranscriptAnchorLimit = 64

func (s *NotifySubscription) Done() <-chan struct{} { return s.done }

func (s *NotifySubscription) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *NotifySubscription) send(message notifyWire) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(notifyWriteTimeout)
	if ctxDeadline, ok := s.ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		_ = s.Close()
		return err
	}
	cancelled := make(chan struct{})
	stopCancel := context.AfterFunc(s.ctx, func() {
		// gorilla/websocket permits Close concurrently with the sole writer. A
		// deadline mutation is itself a write-side transport operation and can race
		// WriteJSON, so cancellation aborts the captured socket instead.
		_ = s.conn.Close()
		if s.afterWriteCancel != nil {
			s.afterWriteCancel()
		}
		close(cancelled)
	})
	err := s.conn.WriteJSON(message)
	if !stopCancel() {
		<-cancelled
	}
	if err != nil {
		_ = s.Close()
	}
	return err
}

func (s *NotifySubscription) StartUtterance(utteranceID string) error {
	return s.StartUtteranceForTranscript(utteranceID, 0, nil)
}

// StartUtteranceForTranscript starts a reply correlated to the final transcript
// that triggered it. timing may contain the work completed before dispatch;
// callers can supply the fuller breakdown again on EndUtteranceWithTiming.
func (s *NotifySubscription) StartUtteranceForTranscript(utteranceID string, transcriptionID uint64, timing *ApplicationTurnTiming) error {
	wireTiming := applicationTimingWire(timing)
	if dispatch, ok := s.consumeTranscriptAnchor(transcriptionID); ok {
		if wireTiming == nil {
			wireTiming = &applicationTurnTimingWire{}
		}
		wireTiming.DispatchMs = time.Since(dispatch).Milliseconds()
	}
	return s.send(notifyWire{Type: notifyMsgUtteranceStart, UtteranceID: utteranceID, TranscriptionID: transcriptionID, ApplicationTiming: wireTiming})
}

func (s *NotifySubscription) recordTranscriptAnchor(id uint64, at time.Time) {
	if id == 0 {
		return
	}
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	if s.transcriptAnchors == nil {
		s.transcriptAnchors = make(map[uint64]time.Time)
	}
	if _, exists := s.transcriptAnchors[id]; !exists {
		s.transcriptOrder = append(s.transcriptOrder, id)
	}
	s.transcriptAnchors[id] = at
	for len(s.transcriptOrder) > notifyTranscriptAnchorLimit {
		delete(s.transcriptAnchors, s.transcriptOrder[0])
		s.transcriptOrder = s.transcriptOrder[1:]
	}
}

func (s *NotifySubscription) consumeTranscriptAnchor(id uint64) (time.Time, bool) {
	if id == 0 {
		return time.Time{}, false
	}
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	at, ok := s.transcriptAnchors[id]
	delete(s.transcriptAnchors, id)
	return at, ok
}

func (s *NotifySubscription) SendUtteranceText(utteranceID, text string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceText, UtteranceID: utteranceID, Text: text})
}

func (s *NotifySubscription) EndUtterance(utteranceID string) error {
	return s.EndUtteranceWithTiming(utteranceID, nil)
}

// EndUtteranceWithTiming closes the input and reports the application's final
// duration breakdown. Argus treats these as customer-measured diagnostics.
func (s *NotifySubscription) EndUtteranceWithTiming(utteranceID string, timing *ApplicationTurnTiming) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceEnd, UtteranceID: utteranceID, ApplicationTiming: applicationTimingWire(timing)})
}

func applicationTimingWire(timing *ApplicationTurnTiming) *applicationTurnTimingWire {
	if timing == nil {
		return nil
	}
	return &applicationTurnTimingWire{
		DispatchMs:        durationMs(timing.Dispatch),
		ModelQueueMs:      durationMs(timing.ModelQueue),
		ModelTTFTMs:       durationMs(timing.ModelTTFT),
		ModelGenerationMs: durationMs(timing.ModelGeneration),
		ModelTotalMs:      durationMs(timing.ModelTotal),
		ResponseBufferMs:  durationMs(timing.ResponseBuffer),
		TotalMs:           durationMs(timing.Total),
	}
}

func durationMs(value time.Duration) int64 {
	if value <= 0 {
		return 0
	}
	return value.Milliseconds()
}

func (s *NotifySubscription) CancelUtterance(utteranceID string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceCancel, UtteranceID: utteranceID})
}

func (s *NotifySubscription) CancelSpeech(scope string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceCancel, Scope: scope})
}

// Configure sets the realtime agent's system prompt and engages it. It preserves
// the original prompt-only SDK surface; configuring this way clears any previously
// declared tools, just like a full configuration with an empty tool set.
func (s *NotifySubscription) Configure(instructions string) error {
	return s.ConfigureAgent(AgentConfiguration{Instructions: instructions})
}

// ConfigureAgent sets the realtime agent's complete behavior and engages it. It
// is the first command a realtime-mode stream sends; before it, the agent is dormant
// and produces no response. Sending it again is a full replacement of the prompt
// and callable tool surface for subsequent turns.
func (s *NotifySubscription) ConfigureAgent(cfg AgentConfiguration) error {
	return s.send(notifyWire{
		Type:          notifyMsgAgentConfigure,
		Instructions:  cfg.Instructions,
		Tools:         toToolWire(cfg.Tools),
		ToolChoice:    cfg.ToolChoice,
		ToolTimeoutMs: int(cfg.ToolTimeout / time.Millisecond),
	})
}

// SubmitToolResult returns a tool call's result to the agent, correlated by callID
// (from the AgentToolCall). It resumes the interrupted turn so the agent can use the
// result; when the agent made several parallel calls, submit each one's result and
// the turn resumes once all are in.
func (s *NotifySubscription) SubmitToolResult(callID, output string) error {
	return s.send(notifyWire{Type: notifyMsgAgentToolResult, CallID: callID, Output: output})
}

// SubmitToolError returns a tool call's failure to the agent, correlated by callID,
// so the agent can react to the error rather than waiting out the call's deadline.
func (s *NotifySubscription) SubmitToolError(callID, errText string) error {
	return s.send(notifyWire{Type: notifyMsgAgentToolResult, CallID: callID, Error: errText})
}

// PushWorker sends a background worker's content to the agent (see WorkerUpdate). The
// media server's attention arbiter decides whether it is spoken now, held as a
// context notice, or suppressed, reconciled against the live turn. Tools stay
// synchronous; this is the only asynchronous lane. Use it for a deferred result (a
// tool that returned "being prepared") or an unprompted observation. Traits are
// honored when the strand is first created (its first push).
func (s *NotifySubscription) PushWorker(u WorkerUpdate) error {
	w := notifyWire{
		Type:       notifyMsgWorkerPush,
		WorkerKind: u.WorkerKind,
		StrandID:   u.StrandID,
		Readiness:  string(u.Readiness),
		Content:    u.Content,
		Awaited:    u.Awaited,
	}
	if u.Traits != nil {
		w.Traits = &traitsWire{
			Multiplicity:   string(u.Traits.Multiplicity),
			Floor:          string(u.Traits.Floor),
			Priority:       u.Traits.Priority,
			NoticeEnvelope: string(u.Traits.NoticeEnvelope),
			ConcurrentCap:  u.Traits.ConcurrentCap,
		}
	}
	return s.send(w)
}

// toToolWire converts the SDK tool declarations to the wire shape.
func toToolWire(tools []AgentTool) []toolWire {
	if len(tools) == 0 {
		return nil
	}
	out := make([]toolWire, 0, len(tools))
	for _, t := range tools {
		out = append(out, toolWire{Name: t.Name, Description: t.Description, Parameters: t.Parameters})
	}
	return out
}

// UpdatePrompt replaces the realtime agent's system prompt for subsequent turns,
// leaving its voice unchanged. Honored only after Configure.
func (s *NotifySubscription) UpdatePrompt(instructions string) error {
	return s.send(notifyWire{Type: notifyMsgAgentPromptUpdate, Instructions: instructions})
}

// SendMessage injects a conversation item into the realtime agent's context with
// the given role (AgentRole*). When respond is true it also asks the agent to
// speak now; otherwise the item is context for the agent's next turn. Honored only
// after Configure.
func (s *NotifySubscription) SendMessage(role, content string, respond bool) error {
	return s.send(notifyWire{Type: notifyMsgAgentMessage, Role: role, Content: content, Respond: &respond})
}

func (s *NotifySubscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.toolCalls.close()
		err = s.conn.Close()
	})
	return err
}

// Subscribe opens a change-notification WebSocket to a regional frame gateway
// for a single stream and dispatches events to handlers until the context is
// cancelled, the stream ends, or the connection is superseded. An unexpected
// transport loss after the initial connection is retried on the same customer
// node with bounded backoff. It blocks for the lifetime of the subscription;
// run it in its own goroutine.
//
// gatewayURL is the winning regional signaling URL (argus-js
// selectedGatewayURL, as an http(s) or ws(s) URL). controlToken is the server-only
// token returned by JoinStream and retained by the customer server.
//
// Because the connection is the subscription, the customer server holds exactly
// one notify socket per stream, and it lands on whichever node the browser
// selected as the stream's region — no cross-node fan-out is required.
func (c *Client) Subscribe(ctx context.Context, gatewayURL, streamID, controlToken string, opts *NotifyOptions, handlers NotifyHandlers) error {
	wsURL, err := notifyWSURL(gatewayURL, opts, handlers.OnFrame != nil)
	if err != nil {
		return err
	}

	dialer := c.wsDialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	connected := false
	backoff := notifyReconnectMinBackoff
	toolCalls := newToolCallDispatcher(ctx, handlers.OnAgentToolCall, handlers.OnAgentToolFailure)
	defer toolCalls.close()
	for {
		header := http.Header{"Authorization": []string{"Bearer " + controlToken}}
		conn, resp, dialErr := dialer.DialContext(ctx, wsURL, header)
		if dialErr != nil {
			err := notifyHandshakeError(resp, dialErr)
			// Initial setup failures retain the prompt-error behavior callers rely on.
			// Once a subscription has been established, transport failures are retried
			// on this same customer node until the context ends. An HTTP response is a
			// definitive gateway rejection (auth, ownership, availability), not an
			// intermittent socket loss, so surface it immediately.
			if !connected || resp != nil {
				return err
			}
			if err := waitNotifyReconnect(ctx, backoff); err != nil {
				return err
			}
			backoff = min(backoff*2, notifyReconnectMaxBackoff)
			continue
		}

		connected = true
		backoff = notifyReconnectMinBackoff
		terminal, readErr := readNotifyConnection(ctx, conn, streamID, handlers, toolCalls)
		_ = conn.Close()
		if terminal {
			return readErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitNotifyReconnect(ctx, backoff); err != nil {
			return err
		}
		backoff = min(backoff*2, notifyReconnectMaxBackoff)
	}
}

// OpenNotify opens a live bidirectional notify subscription. It is the API used
// by customer servers that stream assistant utterances while independently
// observing their lifecycle. Transport reconnection is deliberately left to the
// caller because losing the socket cancels all in-flight utterances.
func (c *Client) OpenNotify(ctx context.Context, gatewayURL, streamID, controlToken string, opts *NotifyOptions, handlers NotifyHandlers) (*NotifySubscription, error) {
	wsURL, err := notifyWSURL(gatewayURL, opts, handlers.OnFrame != nil)
	if err != nil {
		return nil, err
	}
	dialer := c.wsDialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	header := http.Header{"Authorization": []string{"Bearer " + controlToken}}
	conn, response, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, notifyHandshakeError(response, err)
	}
	toolCalls := newToolCallDispatcher(ctx, handlers.OnAgentToolCall, handlers.OnAgentToolFailure)
	subscription := &NotifySubscription{conn: conn, ctx: ctx, toolCalls: toolCalls, done: make(chan struct{}), transcriptAnchors: make(map[uint64]time.Time)}
	originalTranscriptHandler := handlers.OnTranscript
	handlers.OnTranscript = func(text string, transcriptionID uint64) {
		subscription.recordTranscriptAnchor(transcriptionID, time.Now())
		if originalTranscriptHandler != nil {
			originalTranscriptHandler(text, transcriptionID)
		}
	}
	go func() {
		_, readErr := readNotifyConnection(ctx, conn, streamID, handlers, toolCalls)
		subscription.errMu.Lock()
		subscription.err = readErr
		subscription.errMu.Unlock()
		_ = subscription.Close()
		close(subscription.done)
	}()
	return subscription, nil
}

const (
	notifyReconnectMinBackoff = 100 * time.Millisecond
	notifyReconnectMaxBackoff = 5 * time.Second
	maxConcurrentToolCalls    = 16
	maxQueuedToolCallbacks    = 256
)

// toolCallDispatcher keeps potentially synchronous customer functions off the
// WebSocket reader while bounding the number of callbacks a peer can run at once.
// One dispatcher spans transparent reconnects for a Subscribe call, so repeated
// transport loss cannot reset the concurrency bound.
type toolCallDispatcher struct {
	ctx            context.Context
	cancel         context.CancelFunc
	callHandler    func(AgentToolCall)
	failureHandler func(AgentToolFailure)
	jobs           chan toolCallbackJob
	mu             sync.Mutex
	calls          map[string]*toolCallbackState
	startOnce      sync.Once
	closeOnce      sync.Once
}

type toolCallbackJob struct {
	call    *AgentToolCall
	state   *toolCallbackState
	failure *AgentToolFailure
}

type toolCallbackState struct {
	started           chan struct{}
	failureDispatched bool
}

func newToolCallDispatcher(ctx context.Context, callHandler func(AgentToolCall), failureHandler func(AgentToolFailure)) *toolCallDispatcher {
	if callHandler == nil && failureHandler == nil {
		return nil
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	d := &toolCallDispatcher{
		ctx: dispatchCtx, cancel: cancel, callHandler: callHandler, failureHandler: failureHandler,
		calls: make(map[string]*toolCallbackState),
	}
	return d
}

func (d *toolCallDispatcher) start() bool {
	if d.ctx.Err() != nil {
		return false
	}
	d.startOnce.Do(func() {
		d.jobs = make(chan toolCallbackJob, maxQueuedToolCallbacks)
		go d.run()
	})
	return d.ctx.Err() == nil
}

func (d *toolCallDispatcher) dispatch(call AgentToolCall) bool {
	if d == nil || d.callHandler == nil {
		return true
	}
	if !d.start() {
		return false
	}
	d.mu.Lock()
	state := &toolCallbackState{started: make(chan struct{})}
	d.calls[call.CallID] = state
	d.mu.Unlock()
	select {
	case d.jobs <- toolCallbackJob{call: &call, state: state}:
		return true
	case <-d.ctx.Done():
		d.forget(call.CallID)
		return false
	default:
		d.forget(call.CallID)
		return false
	}
}

func (d *toolCallDispatcher) dispatchFailure(failure AgentToolFailure) bool {
	if d == nil || d.failureHandler == nil {
		return true
	}
	d.mu.Lock()
	if state := d.calls[failure.CallID]; state != nil {
		if state.failureDispatched {
			d.mu.Unlock()
			return true
		}
		state.failureDispatched = true
		d.mu.Unlock()
		go d.deliverActiveFailure(state, failure)
		return true
	}
	d.mu.Unlock()
	if !d.start() {
		return false
	}
	select {
	case d.jobs <- toolCallbackJob{failure: &failure}:
		return true
	case <-d.ctx.Done():
		return false
	default:
		return false
	}
}

// deliverActiveFailure preserves the call-before-failure relationship without
// coupling notification to the customer tool's completion. It exits with the
// subscription if a queued call never starts.
func (d *toolCallDispatcher) deliverActiveFailure(state *toolCallbackState, failure AgentToolFailure) {
	select {
	case <-state.started:
	case <-d.ctx.Done():
		return
	}
	if d.ctx.Err() == nil {
		d.failureHandler(failure)
	}
}

func (d *toolCallDispatcher) run() {
	done := make(chan struct{}, maxConcurrentToolCalls)
	active := 0
	for {
		var jobs <-chan toolCallbackJob
		if active < maxConcurrentToolCalls {
			jobs = d.jobs
		}
		select {
		case <-d.ctx.Done():
			return
		case job := <-jobs:
			active++
			go d.runJob(job, done)
		case <-done:
			active--
		}
	}
}

func (d *toolCallDispatcher) runJob(job toolCallbackJob, done chan<- struct{}) {
	defer func() {
		select {
		case done <- struct{}{}:
		case <-d.ctx.Done():
		}
	}()
	if d.ctx.Err() != nil {
		return
	}
	if job.call != nil {
		close(job.state.started)
		d.callHandler(*job.call)
		d.mu.Lock()
		if d.calls[job.call.CallID] == job.state {
			delete(d.calls, job.call.CallID)
		}
		d.mu.Unlock()
	} else if job.failure != nil {
		d.failureHandler(*job.failure)
	}
}

func (d *toolCallDispatcher) forget(callID string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	delete(d.calls, callID)
	d.mu.Unlock()
}

func (d *toolCallDispatcher) close() {
	if d == nil {
		return
	}
	d.closeOnce.Do(d.cancel)
}

// readNotifyConnection serves one established socket. terminal is false only
// for an unexpected transport loss, which Subscribe reconnects transparently.
func readNotifyConnection(ctx context.Context, conn *websocket.Conn, streamID string, handlers NotifyHandlers, toolCalls *toolCallDispatcher) (terminal bool, result error) {
	stopCancelWatch := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopCancelWatch()

	for {
		var msg notifyWire
		if err := conn.ReadJSON(&msg); err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return false, fmt.Errorf("read notify message: %w", err)
		}
		if msg.Stream != "" && msg.Stream != streamID {
			return true, fmt.Errorf("notify stream mismatch: received %q, expected %q", msg.Stream, streamID)
		}

		switch msg.Type {
		case notifyMsgFrame:
			if msg.Stream == "" {
				return true, fmt.Errorf("notify frame is missing stream identity")
			}
			if handlers.OnFrame == nil {
				continue
			}
			frame, decErr := base64.StdEncoding.DecodeString(msg.FrameBase64)
			if decErr != nil {
				return true, fmt.Errorf("decode notify frame: %w", decErr)
			}
			var ts time.Time
			if msg.Timestamp != "" {
				ts, _ = time.Parse(time.RFC3339, msg.Timestamp)
			}
			handlers.OnFrame(NotifyEvent{
				StreamID:    msg.Stream,
				Track:       msg.Track,
				SSIMScore:   msg.SSIMScore,
				Timestamp:   ts,
				FrameFormat: msg.FrameFormat,
				Frame:       frame,
			})
		case notifyMsgSpeechStarted:
			if handlers.OnSpeechStarted != nil {
				handlers.OnSpeechStarted()
			}
		case notifyMsgTranscript:
			// A realtime agent's own turn transcript carries role "assistant"; the
			// user's speech carries no role. Route them to distinct handlers.
			if msg.Role == AgentRoleAssistant {
				if handlers.OnAgentTranscript != nil {
					handlers.OnAgentTranscript(msg.Text)
				}
			} else if handlers.OnTranscript != nil {
				handlers.OnTranscript(msg.Text, msg.TranscriptionID)
			}
		case notifyMsgNoSpeech:
			if handlers.OnNoSpeech != nil {
				handlers.OnNoSpeech()
			}
		case notifyMsgTranscriptionInterrupted:
			if handlers.OnTranscriptionInterrupted != nil {
				handlers.OnTranscriptionInterrupted()
			}
		case notifyMsgTranscriptionUnavailable:
			if handlers.OnTranscriptionUnavailable != nil {
				handlers.OnTranscriptionUnavailable()
			}
		case notifyMsgUtteranceQueued, notifyMsgUtteranceStarted, notifyMsgUtterancePaused,
			notifyMsgUtteranceResumed, notifyMsgUtteranceFinished, notifyMsgUtteranceCancelled,
			notifyMsgUtteranceFailed, notifyMsgUtteranceRejected:
			if handlers.OnUtterance != nil {
				handlers.OnUtterance(UtteranceEvent{
					Type: msg.Type, UtteranceID: msg.UtteranceID, Reason: msg.Reason,
					DeliveryMode: msg.DeliveryMode, TextComplete: msg.TextComplete != nil && *msg.TextComplete,
				})
			}
		case notifyMsgUserText:
			if handlers.OnUserText != nil {
				handlers.OnUserText(msg.MessageID, msg.Text)
			}
		case notifyMsgAgentEngaged:
			if handlers.OnAgentEngaged != nil {
				handlers.OnAgentEngaged()
			}
		case notifyMsgAgentToolCall:
			if !toolCalls.dispatch(AgentToolCall{CallID: msg.CallID, Name: msg.ToolName, Arguments: msg.Arguments}) {
				return true, fmt.Errorf("agent tool callback queue saturated")
			}
		case notifyMsgAgentToolFailed:
			if !toolCalls.dispatchFailure(AgentToolFailure{CallID: msg.CallID, Reason: msg.Reason}) {
				return true, fmt.Errorf("agent tool callback queue saturated")
			}
		case notifyMsgWorkerPushRejected:
			if handlers.OnWorkerPushRejection != nil {
				handlers.OnWorkerPushRejection(WorkerPushRejection{
					WorkerKind: msg.WorkerKind,
					StrandID:   msg.StrandID,
					Reason:     msg.Reason,
				})
			}
		case notifyMsgAgentError:
			if handlers.OnAgentError != nil {
				handlers.OnAgentError(msg.Reason)
			}
		case notifyMsgTokenExpiring:
			if handlers.OnTokenExpiring != nil {
				handlers.OnTokenExpiring()
			}
		case notifyMsgSuperseded:
			if handlers.OnEnded != nil {
				handlers.OnEnded("superseded")
			}
			return true, nil
		case notifyMsgStreamEnded:
			if handlers.OnEnded != nil {
				handlers.OnEnded("stream_ended")
			}
			return true, nil
		case notifyMsgError:
			return true, &NotifyTerminalError{Reason: msg.Reason}
		}
	}
}

func waitNotifyReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func notifyHandshakeError(resp *http.Response, dialErr error) error {
	if resp == nil {
		return fmt.Errorf("dial notify socket: %w", dialErr)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	detail := strings.TrimSpace(string(body))
	if detail == "" {
		return fmt.Errorf("dial notify socket: unexpected status %d: %w", resp.StatusCode, dialErr)
	}
	return fmt.Errorf("dial notify socket: unexpected status %d: %s: %w", resp.StatusCode, detail, dialErr)
}

// notifyWire mirrors the gateway's notify.Message JSON. Kept local so the client
// module stays free of an internal-package dependency.
type notifyWire struct {
	Type              string                     `json:"type"`
	Stream            string                     `json:"stream,omitempty"`
	Track             string                     `json:"track,omitempty"`
	SSIMScore         float64                    `json:"ssim_score,omitempty"`
	FrameFormat       string                     `json:"frame_format,omitempty"`
	FrameBase64       string                     `json:"frame_base64,omitempty"`
	Timestamp         string                     `json:"timestamp,omitempty"`
	Text              string                     `json:"text,omitempty"`
	Reason            string                     `json:"reason,omitempty"`
	UtteranceID       string                     `json:"utterance_id,omitempty"`
	MessageID         string                     `json:"message_id,omitempty"`
	Scope             string                     `json:"scope,omitempty"`
	DeliveryMode      string                     `json:"delivery_mode,omitempty"`
	TextComplete      *bool                      `json:"text_complete,omitempty"`
	TranscriptionID   uint64                     `json:"transcription_id,omitempty"`
	ApplicationTiming *applicationTurnTimingWire `json:"application_timing,omitempty"`
	// Realtime-agent control (outbound) and lifecycle (inbound).
	Instructions string `json:"instructions,omitempty"`
	Role         string `json:"role,omitempty"`
	Content      string `json:"content,omitempty"`
	Respond      *bool  `json:"respond,omitempty"`
	// Realtime-agent tools (agent_configure outbound; tool call inbound / result
	// outbound).
	Tools         []toolWire `json:"tools,omitempty"`
	ToolChoice    string     `json:"tool_choice,omitempty"`
	ToolTimeoutMs int        `json:"tool_timeout_ms,omitempty"`
	CallID        string     `json:"call_id,omitempty"`
	ToolName      string     `json:"tool_name,omitempty"`
	Arguments     string     `json:"arguments,omitempty"`
	Output        string     `json:"output,omitempty"`
	Error         string     `json:"error,omitempty"`
	// Agent-worker push (outbound).
	WorkerKind string      `json:"worker_kind,omitempty"`
	StrandID   string      `json:"strand_id,omitempty"`
	Readiness  string      `json:"readiness,omitempty"`
	Awaited    bool        `json:"awaited,omitempty"`
	Traits     *traitsWire `json:"traits,omitempty"`
}

// traitsWire mirrors the gateway's notify.WorkerTraits JSON.
type traitsWire struct {
	Multiplicity   string `json:"multiplicity,omitempty"`
	Floor          string `json:"floor,omitempty"`
	Priority       int    `json:"priority,omitempty"`
	NoticeEnvelope string `json:"notice_envelope,omitempty"`
	ConcurrentCap  int    `json:"concurrent_cap,omitempty"`
}

// toolWire mirrors the gateway's notify.Tool JSON.
type toolWire struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type applicationTurnTimingWire struct {
	DispatchMs        int64 `json:"dispatch_ms,omitempty"`
	ModelQueueMs      int64 `json:"model_queue_ms,omitempty"`
	ModelTTFTMs       int64 `json:"model_ttft_ms,omitempty"`
	ModelGenerationMs int64 `json:"model_generation_ms,omitempty"`
	ModelTotalMs      int64 `json:"model_total_ms,omitempty"`
	ResponseBufferMs  int64 `json:"response_buffer_ms,omitempty"`
	TotalMs           int64 `json:"total_ms,omitempty"`
}

const (
	notifyMsgFrame                    = "frame"
	notifyMsgSuperseded               = "superseded"
	notifyMsgStreamEnded              = "stream_ended"
	notifyMsgTokenExpiring            = "token_expiring"
	notifyMsgError                    = "error"
	notifyMsgSpeechStarted            = "speech_started"
	notifyMsgTranscript               = "transcript"
	notifyMsgNoSpeech                 = "no_speech"
	notifyMsgTranscriptionInterrupted = "transcription_interrupted"
	notifyMsgTranscriptionUnavailable = "transcription_unavailable"
	notifyMsgUtteranceStart           = "utterance_start"
	notifyMsgUtteranceText            = "utterance_text"
	notifyMsgUtteranceEnd             = "utterance_end"
	notifyMsgUtteranceCancel          = "utterance_cancel"
	notifyMsgUtteranceQueued          = "utterance_queued"
	notifyMsgUtteranceStarted         = "utterance_started"
	notifyMsgUtterancePaused          = "utterance_paused"
	notifyMsgUtteranceResumed         = "utterance_resumed"
	notifyMsgUtteranceFinished        = "utterance_finished"
	notifyMsgUtteranceCancelled       = "utterance_cancelled"
	notifyMsgUtteranceFailed          = "utterance_failed"
	notifyMsgUtteranceRejected        = "utterance_rejected"
	notifyMsgUserText                 = "user_text"
	notifyMsgAgentConfigure           = "agent_configure"
	notifyMsgAgentPromptUpdate        = "agent_prompt_update"
	notifyMsgAgentMessage             = "agent_message"
	notifyMsgAgentEngaged             = "agent_engaged"
	notifyMsgAgentError               = "agent_error"
	notifyMsgAgentToolCall            = "agent_tool_call"
	notifyMsgAgentToolResult          = "agent_tool_result"
	notifyMsgAgentToolFailed          = "agent_tool_failed"
	notifyMsgWorkerPush               = "worker_push"
	notifyMsgWorkerPushRejected       = "worker_push_rejected"
)

// Realtime-agent injected-message roles for NotifySubscription.SendMessage.
const (
	AgentRoleUser      = "user"
	AgentRoleSystem    = "system"
	AgentRoleAssistant = "assistant"
)

// notifyWSURL builds the gateway /notify WebSocket URL, normalizing http(s) to
// ws(s) and attaching watch parameters as query values. Authentication is sent
// separately in the WebSocket handshake's Authorization header.
func notifyWSURL(gatewayURL string, opts *NotifyOptions, watchFrames bool) (string, error) {
	u, err := gatewayBaseURL(gatewayURL, gatewayWebSocket)
	if err != nil {
		return "", err
	}
	u.Path = "/notify"

	q := url.Values{}
	if !watchFrames {
		q.Set("watch_frames", "false")
	}
	if opts != nil {
		if opts.Track != "" {
			q.Set("track", opts.Track)
		}
		if opts.Threshold > 0 {
			q.Set("threshold", strconv.FormatFloat(opts.Threshold, 'f', -1, 64))
		}
		if opts.PollIntervalMs > 0 {
			q.Set("poll_interval_ms", strconv.Itoa(opts.PollIntervalMs))
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
