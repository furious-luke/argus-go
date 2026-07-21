package argus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// defaultFrameTimeout bounds a single FetchFrame request when FrameOptions does
// not override it. Frame decoding is on-demand and usually fast, so this is
// tighter than the Client-wide default.
const defaultFrameTimeout = 10 * time.Second

const (
	frameAgeHeader              = "X-Argus-Frame-Age-Ms"
	staleFrameMaxAttempts       = 5
	defaultStaleFrameRetryDelay = time.Second
	maxStaleFrameRetryDelay     = 2 * time.Second
)

var frameRetryWait = waitForFrameRetry

// ErrStaleFrame is wrapped by StaleFrameError after Argus's bounded automatic
// retries are exhausted.
var ErrStaleFrame = errors.New("argus frame is stale")

// StaleFrameError reports that Argus has media for the requested stream but it
// is too old to present as a current view.
type StaleFrameError struct {
	FrameAge   time.Duration
	RetryAfter time.Duration
}

func (e *StaleFrameError) Error() string {
	return fmt.Sprintf("%s (age %s)", ErrStaleFrame, e.FrameAge.Round(time.Millisecond))
}

func (e *StaleFrameError) Unwrap() error { return ErrStaleFrame }

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
// The returned bytes are the encoded image (JPEG or PNG per opts.Format). A
// stale-frame response is retried for a short fixed window; if media does not
// resume, the returned error matches ErrStaleFrame and can be inspected as a
// *StaleFrameError.
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
	frameClient := c.frameHTTPClient(opts.Timeout)
	for attempt := 0; attempt < staleFrameMaxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+readToken)

		resp, err := frameClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("do request: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusOK {
			return body, nil
		}

		staleErr, stale := staleFrameError(resp)
		if !stale {
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
		}
		if attempt == staleFrameMaxAttempts-1 {
			return nil, staleErr
		}
		if err := frameRetryWait(ctx, staleErr.RetryAfter); err != nil {
			return nil, err
		}
	}

	return nil, ErrStaleFrame
}

func staleFrameError(resp *http.Response) (*StaleFrameError, bool) {
	if resp.StatusCode != http.StatusServiceUnavailable {
		return nil, false
	}
	ageMs, err := strconv.ParseInt(resp.Header.Get(frameAgeHeader), 10, 64)
	if err != nil || ageMs < 0 {
		return nil, false
	}
	retryAfter := defaultStaleFrameRetryDelay
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		retryAfter = time.Duration(seconds) * time.Second
	}
	if retryAfter > maxStaleFrameRetryDelay {
		retryAfter = maxStaleFrameRetryDelay
	}
	return &StaleFrameError{FrameAge: time.Duration(ageMs) * time.Millisecond, RetryAfter: retryAfter}, true
}

func waitForFrameRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for fresh frame: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
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
