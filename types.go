package client

import "time"

// JoinResponse is the payload returned by POST /api/streams.
type JoinResponse struct {
	Token               string `json:"token"`
	ReadToken           string `json:"read_token"`
	StreamID            string `json:"stream_id"`
	Region              string `json:"region"`
	ExpiresAt           string `json:"expires_at"`
	ReadTokenExpiresAt  string `json:"read_token_expires_at,omitempty"`
	MediaServerPublicIP string `json:"media_server_public_ip,omitempty"`
	SignalingURL        string `json:"signaling_url,omitempty"`
	TurnURL             string `json:"turn_url,omitempty"`
	TurnUsername        string `json:"turn_username,omitempty"`
	TurnCredential      string `json:"turn_credential,omitempty"`
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
