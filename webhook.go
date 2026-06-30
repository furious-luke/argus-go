package client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// WebhookSignatureHeader carries the HMAC-SHA256 signature of the signed
	// material, prefixed with "sha256=".
	WebhookSignatureHeader = "X-Argus-Signature"
	// WebhookTimestampHeader carries the unix timestamp (seconds) included in the
	// signed material to prevent replay.
	WebhookTimestampHeader = "X-Argus-Timestamp"

	// defaultWebhookTolerance bounds how old a signed webhook may be before it is
	// rejected as a potential replay.
	defaultWebhookTolerance = 5 * time.Minute
)

// Errors returned by ParseWebhook. Callers can match these with errors.Is to
// distinguish a forged/stale delivery (which should be rejected with 4xx) from a
// malformed body.
var (
	// ErrMissingSignature indicates the request carried no signature header but a
	// secret was supplied for verification.
	ErrMissingSignature = errors.New("argus webhook: missing signature header")
	// ErrInvalidSignature indicates the signature did not match the body.
	ErrInvalidSignature = errors.New("argus webhook: signature mismatch")
	// ErrStaleWebhook indicates the signed timestamp is outside the tolerance.
	ErrStaleWebhook = errors.New("argus webhook: timestamp outside tolerance")
	// ErrMalformedWebhook indicates the body could not be parsed.
	ErrMalformedWebhook = errors.New("argus webhook: malformed payload")
)

// WebhookEvent is a decoded change-trigger webhook delivered to a customer's
// endpoint by an Argus media server.
type WebhookEvent struct {
	// StreamID is the UUID of the stream that changed.
	StreamID string
	// Track is the logical track that changed ("camera" or "screen").
	Track string
	// Timestamp is when the change was detected.
	Timestamp time.Time
	// SSIMScore is the structural-similarity score against the previous baseline
	// frame; lower means a larger change.
	SSIMScore float64
	// FrameFormat is the encoding of Frame (currently always "jpeg").
	FrameFormat string
	// Frame is the decoded image bytes that tripped the trigger.
	Frame []byte
}

// webhookPayloadWire mirrors the JSON body posted by the media server.
type webhookPayloadWire struct {
	StreamUUID  string  `json:"stream_uuid"`
	Track       string  `json:"track"`
	Timestamp   string  `json:"timestamp"`
	SSIMScore   float64 `json:"ssim_score"`
	FrameFormat string  `json:"frame_format"`
	FrameBase64 string  `json:"frame_base64"`
}

// WebhookOption configures ParseWebhook.
type WebhookOption func(*webhookOptions)

type webhookOptions struct {
	tolerance time.Duration
	now       func() time.Time
}

// WithTolerance overrides how old a signed webhook may be before it is rejected
// (default 5 minutes). A non-positive tolerance disables the staleness check.
func WithTolerance(d time.Duration) WebhookOption {
	return func(o *webhookOptions) { o.tolerance = d }
}

// ParseWebhook verifies and decodes a change-trigger webhook delivery.
//
// When secret is non-empty the request signature is verified against the raw
// body using the same scheme the media server signs with — HMAC-SHA256 over
// "<X-Argus-Timestamp>.<body>" — and the signed timestamp is checked against the
// tolerance window to reject replays. Pass the secret returned in
// JoinResponse.WebhookSecret. When secret is empty, signature verification is
// skipped (use only if deliveries are authenticated by other means).
//
// body must be the exact bytes received; re-marshalling will invalidate the
// signature.
func ParseWebhook(secret string, header http.Header, body []byte, opts ...WebhookOption) (*WebhookEvent, error) {
	options := webhookOptions{tolerance: defaultWebhookTolerance, now: time.Now}
	for _, o := range opts {
		o(&options)
	}

	if secret != "" {
		if err := verifySignature(secret, header, body, options); err != nil {
			return nil, err
		}
	}

	var wire webhookPayloadWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedWebhook, err)
	}

	frame, err := base64.StdEncoding.DecodeString(wire.FrameBase64)
	if err != nil {
		return nil, fmt.Errorf("%w: decode frame: %v", ErrMalformedWebhook, err)
	}

	var ts time.Time
	if wire.Timestamp != "" {
		ts, err = time.Parse(time.RFC3339, wire.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("%w: parse timestamp: %v", ErrMalformedWebhook, err)
		}
	}

	return &WebhookEvent{
		StreamID:    wire.StreamUUID,
		Track:       wire.Track,
		Timestamp:   ts,
		SSIMScore:   wire.SSIMScore,
		FrameFormat: wire.FrameFormat,
		Frame:       frame,
	}, nil
}

func verifySignature(secret string, header http.Header, body []byte, options webhookOptions) error {
	sig := header.Get(WebhookSignatureHeader)
	tsHeader := header.Get(WebhookTimestampHeader)
	if sig == "" || tsHeader == "" {
		return ErrMissingSignature
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsHeader))
	mac.Write([]byte("."))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return ErrInvalidSignature
	}

	if options.tolerance > 0 {
		secs, err := strconv.ParseInt(strings.TrimSpace(tsHeader), 10, 64)
		if err != nil {
			return fmt.Errorf("%w: parse signed timestamp: %v", ErrMalformedWebhook, err)
		}
		delta := options.now().Unix() - secs
		if delta < 0 {
			delta = -delta
		}
		if time.Duration(delta)*time.Second > options.tolerance {
			return ErrStaleWebhook
		}
	}

	return nil
}
