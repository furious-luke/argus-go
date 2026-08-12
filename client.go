// Package client is a Go client for the Argus video streaming platform.
//
// Argus is a regionally distributed video ingestion service: end users stream
// video from their browsers to Argus media servers, and application servers
// fetch still frames or short segments on demand whenever an agent needs to
// "see". This package is the server-side half of that story — it is intended to
// run on a customer's own backend, authenticated with the customer's API key.
//
// It covers four things a customer server needs to do:
//
//   - Mint join tokens for new streams, which are handed to a browser so it can
//     publish video (JoinStream / JoinStreamWithOptions).
//   - Fetch decoded frames from a regional frame gateway (FetchFrame).
//   - Subscribe to change notifications for a stream over a persistent WebSocket,
//     receiving the changed frames as they happen (Subscribe).
//   - Stream assistant utterances and observe typed user input over a live,
//     bidirectional notify subscription (OpenNotify).
//
// Subscribe holds one WebSocket per stream to the stream's regional gateway. The
// connection is the subscription: it lands on whichever of the customer's nodes
// opened it, so no cross-node fan-out is needed to route notifications to the
// node serving a given browser.
//
// # Authentication
//
// Four distinct credentials are in play, and the browser and server tokens must not
// be confused:
//
//   - The API key identifies the customer to the control plane. It is
//     long-lived and secret; keep it server-side. The Client sends it as an
//     "Authorization: ApiKey <key>" header.
//   - The join token is a short-lived JWT returned in JoinResponse.Token. It
//     authorizes publishing and is forwarded to the browser. The frame gateway
//     rejects it — it is not a read token.
//   - The read token is a separate short-lived JWT minted by the frame gateway
//     during signaling. It is surfaced to the browser as frameReadToken and
//     relayed back to your server, which presents it to FetchFrame as a bearer
//     token. It is scoped to a single stream and pinned to the serving region.
//   - The control token is returned in JoinResponse.ControlToken and retained
//     only by the customer server. It authenticates Subscribe/OpenNotify and can
//     enqueue paid synthesis; never send it to the browser.
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
//	// 2. The browser connects, receives its read token (frameReadToken), and
//	//    relays it and the winning gateway URL (selectedGatewayURL) back to
//	//    your server.
//
//	// 3. Fetch a frame from the stream's gateway with that read token.
//	frame, err := c.FetchFrame(ctx, selectedGatewayURL, join.StreamID, readToken, nil)
package argus

import (
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// defaultTimeout is the request timeout applied by New. Callers needing a
// different timeout (or transport, proxy, etc.) should use NewWithHTTPClient.
const defaultTimeout = 30 * time.Second

// Client talks to the Argus control plane and frame gateways on behalf of a
// single customer account. It is safe for concurrent use by multiple
// goroutines.
type Client struct {
	baseURL  string
	apiKey   string
	client   *http.Client
	wsDialer *websocket.Dialer
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
// caller control timeouts, transport, proxies, and TLS configuration. Standard
// *http.Transport proxy/dial/TLS settings are also applied to Subscribe's
// WebSocket handshake. The client must not be nil.
func NewWithHTTPClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		apiKey:   apiKey,
		client:   httpClient,
		wsDialer: websocketDialerForHTTPClient(httpClient),
	}
}

// websocketDialerForHTTPClient carries the standard transport settings that
// also apply to a WebSocket handshake. This keeps Subscribe consistent with
// NewWithHTTPClient for proxies, custom network dialers, TLS roots, client
// certificates, and cookie jars.
func websocketDialerForHTTPClient(httpClient *http.Client) *websocket.Dialer {
	dialer := *websocket.DefaultDialer
	if httpClient == nil {
		return &dialer
	}
	dialer.Jar = httpClient.Jar

	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if t, ok := transport.(*http.Transport); ok {
		dialer.Proxy = t.Proxy
		dialer.NetDialContext = t.DialContext
		dialer.NetDialTLSContext = t.DialTLSContext
		if t.TLSClientConfig != nil {
			tlsConfig := t.TLSClientConfig.Clone()
			// The HTTP transport advertises "h2" via ALPN (Go appends it to
			// NextProtos when it enables HTTP/2, e.g. via ForceAttemptHTTP2).
			// A WebSocket handshake runs over HTTP/1.1, so carrying that ALPN
			// list into the dial lets the server negotiate h2 and reply with
			// HTTP/2 frames that gorilla/websocket cannot parse ("protocol
			// \"h2\" was given but is not supported"). Clear it so the
			// handshake negotiates HTTP/1.1.
			tlsConfig.NextProtos = nil
			dialer.TLSClientConfig = tlsConfig
		}
		if t.TLSHandshakeTimeout > 0 {
			dialer.HandshakeTimeout = t.TLSHandshakeTimeout
		}
	}
	return &dialer
}
