package argus

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type frameRoundTripFunc func(*http.Request) (*http.Response, error)

func (f frameRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestFetchFrameRetriesStaleResponsesUntilFresh(t *testing.T) {
	originalWait := frameRetryWait
	frameRetryWait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { frameRetryWait = originalWait })

	attempts := 0
	httpClient := &http.Client{Transport: frameRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		if attempts < 3 {
			return frameResponse(http.StatusServiceUnavailable, "frame is stale", map[string]string{
				frameAgeHeader: "16000",
				"Retry-After":  "1",
			}), nil
		}
		return frameResponse(http.StatusOK, "fresh-image", nil), nil
	})}
	client := NewWithHTTPClient("https://control.example", "key", httpClient)

	frame, err := client.FetchFrame(context.Background(), "https://gateway.example", "stream-1", "read-token", nil)

	if err != nil {
		t.Fatalf("FetchFrame: %v", err)
	}
	if string(frame) != "fresh-image" || attempts != 3 {
		t.Fatalf("frame=%q attempts=%d", frame, attempts)
	}
}

func TestFetchFrameReturnsTypedErrorAfterStaleRetries(t *testing.T) {
	originalWait := frameRetryWait
	frameRetryWait = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { frameRetryWait = originalWait })

	attempts := 0
	httpClient := &http.Client{Transport: frameRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		return frameResponse(http.StatusServiceUnavailable, "frame is stale", map[string]string{
			frameAgeHeader: "23000",
			"Retry-After":  "1",
		}), nil
	})}
	client := NewWithHTTPClient("https://control.example", "key", httpClient)

	_, err := client.FetchFrame(context.Background(), "https://gateway.example", "stream-1", "read-token", nil)

	if !errors.Is(err, ErrStaleFrame) {
		t.Fatalf("error = %v, want ErrStaleFrame", err)
	}
	var stale *StaleFrameError
	if !errors.As(err, &stale) || stale.FrameAge != 23*time.Second {
		t.Fatalf("typed stale error = %#v", stale)
	}
	if attempts != staleFrameMaxAttempts {
		t.Fatalf("attempts = %d, want %d", attempts, staleFrameMaxAttempts)
	}
}

func frameResponse(status int, body string, headers map[string]string) *http.Response {
	header := make(http.Header)
	for key, value := range headers {
		header.Set(key, value)
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
