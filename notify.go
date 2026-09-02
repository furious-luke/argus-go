package argus

import (
	"context"
	"encoding/base64"
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
}

type UtteranceEvent struct {
	Type         string
	UtteranceID  string
	Reason       string
	DeliveryMode string
	TextComplete bool
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
	writeMu   sync.Mutex
	done      chan struct{}
	errMu     sync.Mutex
	err       error
	closeOnce sync.Once
	// afterWriteCancel is a test-only completion barrier for the cancellation
	// callback installed around a blocked write. Nil in production.
	afterWriteCancel func()
}

const notifyWriteTimeout = time.Second

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
	return s.send(notifyWire{Type: notifyMsgUtteranceStart, UtteranceID: utteranceID})
}

func (s *NotifySubscription) SendUtteranceText(utteranceID, text string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceText, UtteranceID: utteranceID, Text: text})
}

func (s *NotifySubscription) EndUtterance(utteranceID string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceEnd, UtteranceID: utteranceID})
}

func (s *NotifySubscription) CancelUtterance(utteranceID string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceCancel, UtteranceID: utteranceID})
}

func (s *NotifySubscription) CancelSpeech(scope string) error {
	return s.send(notifyWire{Type: notifyMsgUtteranceCancel, Scope: scope})
}

func (s *NotifySubscription) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.conn.Close() })
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
		terminal, readErr := readNotifyConnection(ctx, conn, streamID, handlers)
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
	subscription := &NotifySubscription{conn: conn, ctx: ctx, done: make(chan struct{})}
	go func() {
		_, readErr := readNotifyConnection(ctx, conn, streamID, handlers)
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
)

// readNotifyConnection serves one established socket. terminal is false only
// for an unexpected transport loss, which Subscribe reconnects transparently.
func readNotifyConnection(ctx context.Context, conn *websocket.Conn, streamID string, handlers NotifyHandlers) (terminal bool, result error) {
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
			if handlers.OnTranscript != nil {
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
	Type            string  `json:"type"`
	Stream          string  `json:"stream,omitempty"`
	Track           string  `json:"track,omitempty"`
	SSIMScore       float64 `json:"ssim_score,omitempty"`
	FrameFormat     string  `json:"frame_format,omitempty"`
	FrameBase64     string  `json:"frame_base64,omitempty"`
	Timestamp       string  `json:"timestamp,omitempty"`
	Text            string  `json:"text,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	UtteranceID     string  `json:"utterance_id,omitempty"`
	MessageID       string  `json:"message_id,omitempty"`
	Scope           string  `json:"scope,omitempty"`
	DeliveryMode    string  `json:"delivery_mode,omitempty"`
	TextComplete    *bool   `json:"text_complete,omitempty"`
	TranscriptionID uint64  `json:"transcription_id,omitempty"`
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
