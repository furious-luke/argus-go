package client

import "time"

// JoinResponse is the payload returned by POST /api/streams.
type JoinResponse struct {
	Token       string   `json:"token"`
	StreamID    string   `json:"stream_id"`
	ExpiresAt   string   `json:"expires_at"`
	GatewayURLs []string `json:"gateway_urls"`
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
