package argus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// JoinResponse is the result of creating a stream via JoinStream. The Token and
// GatewayURLs are meant to be forwarded to the end user's browser so it can
// publish video; the customer server retains StreamID and ControlToken.
type JoinResponse struct {
	// Token is the short-lived join JWT. Forward it to the browser to publish.
	// It is NOT the read token — the frame gateway rejects it. The read token
	// for FetchFrame is minted by the gateway during signaling and relayed back
	// from the browser (see FetchFrame).
	Token string `json:"token"`
	// StreamID is the Argus-generated UUID of the new stream.
	StreamID string `json:"stream_id"`
	// ExpiresAt is the token expiry as an RFC 3339 timestamp.
	ExpiresAt string `json:"expires_at"`
	// GatewayURLs are regional signaling URLs for this stream, in preference
	// order. The browser races them; relay its selectedGatewayURL back with the
	// read token. FetchFrame and Subscribe accept that signaling URL directly and
	// normalize it to the appropriate gateway endpoint.
	GatewayURLs []string `json:"gateway_urls"`
	// ControlToken is retained by the customer server and authenticates its
	// bidirectional /notify socket. It must never be sent to the browser.
	ControlToken          string `json:"control_token"`
	ControlTokenExpiresAt string `json:"control_token_expires_at"`
}

type ControlTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

// Stable stream-creation error codes carried by a StreamJoinError.Code. They
// mirror the codes Argus emits on the wire, so callers can classify a rejection
// against a named constant — notably to detect a stale voice catalogue and
// refetch it — instead of hard-coding the literal. The wire values are pinned
// by TestSpec_StreamJoinCodes_PinWireValues on both sides of the boundary.
const (
	// StreamJoinCodeStaleCatalogVersion means the request named a no-longer-current
	// voice catalogue version. Refetch the catalogue and retry.
	StreamJoinCodeStaleCatalogVersion = "stale_catalog_version"
	// StreamJoinCodeInvalidVoiceConfig is the category for a rejected voice
	// configuration whose specific reason is otherwise opaque to the caller.
	StreamJoinCodeInvalidVoiceConfig = "invalid_voice_config"
)

// StreamJoinError describes a control-plane rejection of stream creation. Code
// is the stable API error code when Argus supplied one (see StreamJoinCode*);
// callers can safely distinguish a stale voice catalogue from failures whose
// request outcome may be unknown (such as a transport timeout).
type StreamJoinError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *StreamJoinError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("create stream: unexpected status %d (%s): %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("create stream: unexpected status %d: %s", e.StatusCode, e.Message)
}

// StreamJoinErrorCode exposes the server's stable error code without coupling
// consumers to this concrete error type.
func (e *StreamJoinError) StreamJoinErrorCode() string { return e.Code }

// PublisherTokenResponse is a browser-safe replacement join bundle for an
// existing stream. It lets a new publisher take over without recreating the
// stream and therefore preserves its immutable voice and STT configuration.
type PublisherTokenResponse struct {
	Token       string   `json:"token"`
	StreamID    string   `json:"stream_id"`
	ExpiresAt   string   `json:"expires_at"`
	GatewayURLs []string `json:"gateway_urls"`
}

// JoinOptions configures a JoinStream request. A nil *JoinOptions selects an
// eligible region automatically.
type JoinOptions struct {
	// Region pins the stream to a specific region slug. Empty lets the control
	// plane select an eligible region (subject to data-residency policy).
	Region string
	// Language is an initial language / BCP-47 hint for transcription, applied if
	// the stream publishes a microphone track. Empty defers to provider defaults.
	// It is stored server-side on the stream and never exposed to the browser.
	Language string
	// Keyterms boost recognition of domain vocabulary during transcription.
	// Server-side only, like Language.
	Keyterms []string
	// RecordingEnabled opts this stream into recording to object storage for later
	// review (separate video/mic/speech tracks tied to one timeline by a manifest).
	// It is not part of the live read path.
	RecordingEnabled bool
	// RecordingRetentionDays is how long recordings are retained before deletion.
	// Zero defers to the server default. Ignored unless RecordingEnabled is set.
	RecordingRetentionDays int
	// StorageRegion pins where recordings are stored at rest, decoupled from the
	// processing region, for data-residency or cost reasons. Empty stores in the
	// processing region. It requires RecordingEnabled and must be a
	// residency-compatible region with usable storage, else the create is rejected.
	StorageRegion string
	// Voice is the optional text-to-speech voice configuration for the stream. It
	// is validated against the catalog and pinned at creation. Nil means the
	// fleet-default voices. Discover valid selections with GetVoices. It is
	// server-side only, like the transcription options.
	Voice *VoiceConfig
}

// RefreshControlToken replaces an expiring control token. Once placement has
// completed the control plane narrows the new token to the fixed region.
func (c *Client) RefreshControlToken(ctx context.Context, streamID string) (*ControlTokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/streams/"+streamID+"/control-token", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(message))
	}
	var result ControlTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

// RefreshPublisherToken mints a fresh browser join capability for an existing,
// placed stream. It does not expose or rotate the server-only control token.
func (c *Client) RefreshPublisherToken(ctx context.Context, streamID string) (*PublisherTokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/streams/"+streamID+"/publisher-token", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(message))
	}
	var result PublisherTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

// joinStreamBody mirrors the control plane's createStreamRequest JSON shape.
type joinStreamBody struct {
	Region                 string       `json:"region,omitempty"`
	RecordingEnabled       bool         `json:"recording_enabled,omitempty"`
	RecordingRetentionDays int          `json:"recording_retention_days,omitempty"`
	StorageRegion          string       `json:"storage_region,omitempty"`
	Language               string       `json:"language,omitempty"`
	Keyterms               []string     `json:"keyterms,omitempty"`
	Voice                  *VoiceConfig `json:"voice,omitempty"`
}

// JoinStream creates a new stream with default options and returns its join
// token bundle. It is shorthand for JoinStreamWithOptions(ctx, nil).
func (c *Client) JoinStream(ctx context.Context) (*JoinResponse, error) {
	return c.JoinStreamWithOptions(ctx, nil)
}

// JoinStreamWithOptions creates a new stream with the given options and returns
// its join token bundle. A nil opts is equivalent to JoinStream(ctx).
func (c *Client) JoinStreamWithOptions(ctx context.Context, opts *JoinOptions) (*JoinResponse, error) {
	var body joinStreamBody
	if opts != nil {
		body.Region = opts.Region
		body.RecordingEnabled = opts.RecordingEnabled
		body.RecordingRetentionDays = opts.RecordingRetentionDays
		body.StorageRegion = opts.StorageRegion
		body.Language = opts.Language
		body.Keyterms = opts.Keyterms
		body.Voice = opts.Voice
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/streams", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error   string `json:"error"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(msg, &apiErr)
		message := apiErr.Message
		if message == "" {
			message = string(msg)
		}
		code := apiErr.Code
		if code == "" {
			code = apiErr.Error
		}
		return nil, &StreamJoinError{StatusCode: resp.StatusCode, Code: code, Message: message}
	}

	var jr JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&jr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &jr, nil
}
