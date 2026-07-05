package argus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultFrameTimeout bounds a single FetchFrame request when FrameOptions does
// not override it. Frame decoding is on-demand and usually fast, so this is
// tighter than the Client-wide default.
const defaultFrameTimeout = 10 * time.Second

// FrameOptions configures a FetchFrame request. A nil *FrameOptions uses the
// defaults documented on each field.
type FrameOptions struct {
	// Track is the logical track to read, "camera" or "screen" (default
	// "camera").
	Track string
	// Format is the output image format, "jpeg" or "png" (default "jpeg").
	Format string
	// Timeout is the HTTP timeout for this request (default 10s). It overrides
	// the Client's own timeout for the duration of the call.
	Timeout time.Duration
}

// FetchFrame retrieves a single decoded frame from a regional frame gateway.
//
// gatewayURL is the base URL of the gateway serving the stream — use one of the
// values from JoinResponse.GatewayURLs. streamID is the stream's UUID
// (JoinResponse.StreamID) and readToken is the bearer token authorizing the
// read. The read token is NOT JoinResponse.Token (that is the join token, which
// this endpoint rejects); it is minted by the gateway during signaling, surfaced
// to the browser as frameReadToken, and relayed back to your server. A nil opts
// uses the default track and format.
//
// The returned bytes are the encoded image (JPEG or PNG per opts.Format).
func (c *Client) FetchFrame(ctx context.Context, gatewayURL, streamID, readToken string, opts *FrameOptions) ([]byte, error) {
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

	endpoint := fmt.Sprintf("%s/frames/%s?track=%s&format=%s",
		strings.TrimRight(gatewayURL, "/"),
		url.PathEscape(streamID),
		url.QueryEscape(track),
		url.QueryEscape(format),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+readToken)

	resp, err := c.frameHTTPClient(opts.Timeout).Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(msg))
	}

	return io.ReadAll(resp.Body)
}

// frameHTTPClient returns an *http.Client to use for a frame request. When the
// requested timeout differs from the Client's own, it returns a shallow copy
// with that timeout so the shared transport (connection pool) is preserved.
func (c *Client) frameHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = defaultFrameTimeout
	}
	if timeout == c.client.Timeout {
		return c.client
	}
	cc := *c.client
	cc.Timeout = timeout
	return &cc
}
