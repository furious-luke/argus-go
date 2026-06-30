package client

import "time"

// JoinResponse is the payload returned by POST /api/streams.
type JoinResponse struct {
	Token       string   `json:"token"`
	StreamID    string   `json:"stream_id"`
	ExpiresAt   string   `json:"expires_at"`
	GatewayURLs []string `json:"gateway_urls"`
	// WebhookSecret is the server-generated HMAC signing secret for the stream's
	// change-trigger webhook. Populated only when a trigger was configured on the
	// join request. Keep it to verify signatures on inbound webhook deliveries.
	WebhookSecret string `json:"webhook_secret,omitempty"`
}

// JoinOptions configures a JoinStream request.
type JoinOptions struct {
	// Region pins the stream to a specific region slug. Empty means the control
	// plane selects an eligible region.
	Region string
	// Trigger optionally configures a per-stream change-trigger webhook.
	Trigger *TriggerConfig
}

// TriggerConfig configures a per-stream change-trigger webhook. The signing
// secret is generated server-side and returned in JoinResponse.WebhookSecret.
type TriggerConfig struct {
	// WebhookURL is the absolute http/https URL change events are POSTed to.
	WebhookURL string
	// Threshold, if set, is the change-detection threshold in (0,1].
	Threshold *float64
	// Track, if set, must be "camera" or "screen".
	Track string
	// PollIntervalMs, if set, is the watcher poll interval in milliseconds (>0).
	PollIntervalMs *int
}

// FrameOptions configures a frame fetch request.
type FrameOptions struct {
	// Track is the logical track name (default "camera").
	Track string
	// Format is the output image format: "jpeg" or "png" (default "jpeg").
	Format string
	// Timeout is the HTTP client timeout for the request (default 10s).
	Timeout time.Duration
}
