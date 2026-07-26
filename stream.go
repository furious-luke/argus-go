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

// joinStreamBody mirrors the control plane's createStreamRequest JSON shape.
type joinStreamBody struct {
	Region   string   `json:"region,omitempty"`
	Language string   `json:"language,omitempty"`
	Keyterms []string `json:"keyterms,omitempty"`
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
		body.Language = opts.Language
		body.Keyterms = opts.Keyterms
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
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(msg))
	}

	var jr JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&jr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &jr, nil
}
