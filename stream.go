package client

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
// publish video; the customer server retains StreamID (and WebhookSecret, if a
// trigger was configured) for later reads and webhook verification.
type JoinResponse struct {
	// Token is the short-lived join JWT. Forward it to the browser to publish,
	// and present it as the read token when calling FetchFrame.
	Token string `json:"token"`
	// StreamID is the Argus-generated UUID of the new stream.
	StreamID string `json:"stream_id"`
	// ExpiresAt is the token expiry as an RFC 3339 timestamp.
	ExpiresAt string `json:"expires_at"`
	// GatewayURLs are the regional frame-gateway base URLs for this stream, in
	// preference order. Use one of these as the gatewayURL for FetchFrame.
	GatewayURLs []string `json:"gateway_urls"`
	// WebhookSecret is the server-generated HMAC signing secret for the stream's
	// change-trigger webhook. It is populated only when a Trigger was configured
	// on the join request. Store it to verify signatures on inbound webhook
	// deliveries with ParseWebhook.
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// JoinOptions configures a JoinStream request. A nil *JoinOptions selects an
// eligible region automatically and configures no trigger.
type JoinOptions struct {
	// Region pins the stream to a specific region slug. Empty lets the control
	// plane select an eligible region (subject to data-residency policy).
	Region string
	// Trigger, if set, configures a per-stream change-trigger webhook.
	Trigger *TriggerConfig
}

// TriggerConfig configures a per-stream change-trigger webhook. When set, Argus
// watches the stream for visual changes and POSTs an event to WebhookURL each
// time one is detected. The signing secret is generated server-side and
// returned in JoinResponse.WebhookSecret; decode deliveries with ParseWebhook.
type TriggerConfig struct {
	// WebhookURL is the absolute http/https URL change events are POSTed to.
	WebhookURL string
	// Threshold, if set, is the change-detection threshold in (0,1]. Leave nil to
	// use the server default.
	Threshold *float64
	// Track, if set, must be "camera" or "screen". Empty uses the server default.
	Track string
	// PollIntervalMs, if set, is the watcher poll interval in milliseconds (>0).
	// Leave nil to use the server default.
	PollIntervalMs *int
}

// joinStreamBody mirrors the control plane's createStreamRequest JSON shape.
type joinStreamBody struct {
	Region  string             `json:"region,omitempty"`
	Trigger *joinStreamTrigger `json:"trigger,omitempty"`
}

type joinStreamTrigger struct {
	WebhookURL     string   `json:"webhook_url"`
	Threshold      *float64 `json:"threshold,omitempty"`
	Track          string   `json:"track,omitempty"`
	PollIntervalMs *int     `json:"poll_interval_ms,omitempty"`
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
		if opts.Trigger != nil {
			body.Trigger = &joinStreamTrigger{
				WebhookURL:     opts.Trigger.WebhookURL,
				Threshold:      opts.Trigger.Threshold,
				Track:          opts.Trigger.Track,
				PollIntervalMs: opts.Trigger.PollIntervalMs,
			}
		}
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
