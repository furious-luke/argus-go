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
	"time"

	"github.com/gorilla/websocket"
)

// NotifyEvent is a change notification pushed over a stream's notify WebSocket.
// It carries the frame that tripped change detection (or the initial frame
// delivered on subscribe).
type NotifyEvent struct {
	// StreamID is the UUID of the stream the event concerns.
	StreamID string
	// Track is the logical track that changed ("camera" or "screen").
	Track string
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
	// Track selects which track to watch ("camera" or "screen").
	Track string
	// Threshold is the change-detection threshold in (0,1].
	Threshold float64
	// PollIntervalMs is the watcher poll interval in milliseconds (>0).
	PollIntervalMs int
}

// NotifyHandlers receive events over a subscription's lifetime. All are optional.
type NotifyHandlers struct {
	// OnFrame is called for each frame (initial and subsequent changes).
	OnFrame func(NotifyEvent)
	// OnTokenExpiring fires when the read token is near expiry. The current client
	// does not support replacing the token in place; the subscription ends when
	// the gateway drops the expired connection.
	OnTokenExpiring func()
	// OnEnded fires when the stream ended or this subscription was superseded by a
	// newer one (e.g. the browser reconnected to a different node). After it
	// fires, Subscribe returns.
	OnEnded func(reason string)
}

// Subscribe opens a change-notification WebSocket to a regional frame gateway
// for a single stream and dispatches events to handlers until the context is
// cancelled, the stream ends, or the connection is superseded. An unexpected
// transport loss after the initial connection is retried on the same customer
// node with bounded backoff. It blocks for the lifetime of the subscription;
// run it in its own goroutine.
//
// gatewayURL is the winning regional signaling URL (argus-js
// selectedGatewayURL, as an http(s) or ws(s) URL). readToken is the per-stream read
// token relayed back from the browser (argus-js frameReadToken) — the same
// credential FetchFrame uses.
//
// Because the connection is the subscription, the customer server holds exactly
// one notify socket per stream, and it lands on whichever node the browser
// reported the read token to — no cross-node fan-out is required.
func (c *Client) Subscribe(ctx context.Context, gatewayURL, streamID, readToken string, opts *NotifyOptions, handlers NotifyHandlers) error {
	wsURL, err := notifyWSURL(gatewayURL, readToken, opts)
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
		conn, resp, dialErr := dialer.DialContext(ctx, wsURL, nil)
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
			return true, fmt.Errorf("notify error: %s", msg.Reason)
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
	Type        string  `json:"type"`
	Stream      string  `json:"stream,omitempty"`
	Track       string  `json:"track,omitempty"`
	SSIMScore   float64 `json:"ssim_score,omitempty"`
	FrameFormat string  `json:"frame_format,omitempty"`
	FrameBase64 string  `json:"frame_base64,omitempty"`
	Timestamp   string  `json:"timestamp,omitempty"`
	Reason      string  `json:"reason,omitempty"`
}

const (
	notifyMsgFrame         = "frame"
	notifyMsgSuperseded    = "superseded"
	notifyMsgStreamEnded   = "stream_ended"
	notifyMsgTokenExpiring = "token_expiring"
	notifyMsgError         = "error"
)

// notifyWSURL builds the gateway /notify WebSocket URL, normalizing http(s) to
// ws(s) and attaching the token and watch parameters as query values.
func notifyWSURL(gatewayURL, readToken string, opts *NotifyOptions) (string, error) {
	u, err := gatewayBaseURL(gatewayURL, gatewayWebSocket)
	if err != nil {
		return "", err
	}
	u.Path = "/notify"

	q := url.Values{}
	q.Set("token", readToken)
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
