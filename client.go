// Package client is a Go client for the Argus video streaming platform.
//
// Argus is a regionally distributed video ingestion service: end users stream
// video from their browsers to Argus media servers, and application servers
// fetch still frames or short segments on demand whenever an agent needs to
// "see". This package is the server-side half of that story — it is intended to
// run on a customer's own backend, authenticated with the customer's API key.
//
// It covers three things a customer server needs to do:
//
//   - Mint join tokens for new streams, which are handed to a browser so it can
//     publish video (JoinStream / JoinStreamWithOptions).
//   - Fetch decoded frames from a regional frame gateway (FetchFrame).
//   - Verify and decode change-trigger webhooks delivered by Argus when a
//     stream's video changes (ParseWebhook).
//
// The package depends only on the standard library so it can be vendored or
// imported into external applications without pulling in the rest of Argus.
//
// # Authentication
//
// Two distinct credentials are in play and must not be confused:
//
//   - The API key identifies the customer to the control plane and frame
//     gateway. It is long-lived and secret; keep it server-side. The Client
//     sends it as an "Authorization: ApiKey <key>" header.
//   - The read token is a short-lived JWT returned in JoinResponse.Token (and
//     mirrored for reads). It is scoped to a single stream and is what
//     FetchFrame presents as a bearer token.
//
// # Typical flow
//
//	c := client.New("https://argus.example.com", "argus_api_key_...")
//
//	// 1. Mint a join token and forward it to the browser.
//	join, err := c.JoinStream(ctx)
//	if err != nil {
//		// ...
//	}
//	// hand join.Token and join.GatewayURLs to the end user's browser
//
//	// 2. Later, fetch a frame from the stream's gateway.
//	frame, err := c.FetchFrame(ctx, join.GatewayURLs[0], join.StreamID, join.Token, nil)
package argus

import (
	"net/http"
	"strings"
	"time"
)

// defaultTimeout is the request timeout applied by New. Callers needing a
// different timeout (or transport, proxy, etc.) should use NewWithHTTPClient.
const defaultTimeout = 30 * time.Second

// Client talks to the Argus control plane and frame gateways on behalf of a
// single customer account. It is safe for concurrent use by multiple
// goroutines.
type Client struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// New creates a Client with sensible defaults.
//
// baseURL is the root URL of the Argus control plane (e.g.
// "https://argus.example.com"); a trailing slash is trimmed. apiKey is the
// customer's API key, sent as an "Authorization: ApiKey <key>" header on
// control-plane requests.
func New(baseURL, apiKey string) *Client {
	return NewWithHTTPClient(baseURL, apiKey, &http.Client{Timeout: defaultTimeout})
}

// NewWithHTTPClient is like New but uses the supplied *http.Client, letting the
// caller control timeouts, transport, proxies, and TLS configuration. The
// client must not be nil.
func NewWithHTTPClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  httpClient,
	}
}
