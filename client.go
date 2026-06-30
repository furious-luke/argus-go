// Package client provides a Go client for the Argus streaming platform.
//
// Customer servers use this library to:
//   - Request join tokens for new streams (POST /api/streams)
//   - Fetch decoded video frames from the read gateway
//
// Basic usage:
//
//	c := client.New("https://argus.example.com", "argus_api_key_...")
//	j, err := c.JoinStream(ctx)
//	...
//	frame, err := c.FetchFrame(ctx, gatewayURL, j.StreamID, readToken, nil)
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is an Argus API client.
type Client struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// New creates an Argus client. baseURL should be the root URL of the Argus
// control plane (e.g. "https://argus.example.com"). apiKey is the customer's
// API key used for Authorization: ApiKey <key> headers.
func New(baseURL, apiKey string) *Client {
	return NewWithHTTPClient(baseURL, apiKey, &http.Client{Timeout: 30 * time.Second})
}

// NewWithHTTPClient creates an Argus client with a custom HTTP client.
func NewWithHTTPClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		client:  httpClient,
	}
}

// JoinStream creates a new stream and returns the join token bundle.
// The caller should forward the Token and gateway URLs to the browser.
func (c *Client) JoinStream(ctx context.Context) (*JoinResponse, error) {
	return c.JoinStreamWithOptions(ctx, nil)
}

// joinStreamBody mirrors the server's createStreamRequest JSON shape.
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

// JoinStreamWithOptions creates a new stream with the given options and returns
// the join token bundle. A nil opts is equivalent to JoinStream(ctx).
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

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/streams", bytes.NewReader(payload))
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
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var jr JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&jr); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &jr, nil
}

// FetchFrame retrieves a single decoded frame from the read gateway.
// gatewayURL is the root URL of the Argus frame gateway (e.g.
// "https://gateway.argus.example.com").  The returned bytes are a JPEG or
// PNG image depending on opts.Format.  Use the ReadToken from JoinResponse
// for readToken.
func (c *Client) FetchFrame(ctx context.Context, gatewayURL, streamUUID, readToken string, opts *FrameOptions) ([]byte, error) {
	if opts == nil {
		opts = &FrameOptions{}
	}
	track := opts.Track
	if track == "" {
		track = "camera"
	}
	format := opts.Format
	if format == "" {
		format = "jpeg"
	}
	gatewayURL = strings.TrimRight(gatewayURL, "/")

	url := fmt.Sprintf("%s/frames/%s?track=%s&format=%s", gatewayURL, streamUUID, track, format)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+readToken)

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	client := c.client
	if timeout != client.Timeout {
		// Clone client with per-request timeout.
		cc := *client
		cc.Timeout = timeout
		client = &cc
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}
