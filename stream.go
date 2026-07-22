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
// publish video; the customer server retains StreamID for later reads.
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
}

// JoinOptions configures a JoinStream request. A nil *JoinOptions selects an
// eligible region automatically.
type JoinOptions struct {
	// Region pins the stream to a specific region slug. Empty lets the control
	// plane select an eligible region (subject to data-residency policy).
	Region string
}

// joinStreamBody mirrors the control plane's createStreamRequest JSON shape.
type joinStreamBody struct {
	Region string `json:"region,omitempty"`
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
