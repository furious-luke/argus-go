package argus

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	defaultAPIKey = "argus_api_key_test"
	defaultSecret = "whsec-client-test"
)

// CustomerServerActor drives a Client against a fake control plane and frame
// gateway, standing in for a customer's own backend. The captured requests on
// the embedded fakes let specs assert on what the client actually sent.
type CustomerServerActor struct {
	t            *testing.T
	client       *Client
	controlPlane *fakeControlPlane
	gateway      *fakeGateway
	gatewayURL   string
}

// MustJoin creates a stream with the given options and fails the test on error.
func (a *CustomerServerActor) MustJoin(opts *JoinOptions) *JoinResponse {
	a.t.Helper()
	resp, err := a.client.JoinStreamWithOptions(context.Background(), opts)
	require.NoError(a.t, err)
	return resp
}

// Join attempts to create a stream and returns the error for the spec to assert
// on. Used when the failure is the subject under test.
func (a *CustomerServerActor) Join(opts *JoinOptions) (*JoinResponse, error) {
	return a.client.JoinStreamWithOptions(context.Background(), opts)
}

// MustFetchFrame fetches a frame for the stream and fails the test on error.
func (a *CustomerServerActor) MustFetchFrame(streamID, readToken string, opts *FrameOptions) []byte {
	a.t.Helper()
	frame, err := a.client.FetchFrame(context.Background(), a.gatewayURL, streamID, readToken, opts)
	require.NoError(a.t, err)
	return frame
}

// FetchFrame fetches a frame and returns the error for the spec to assert on.
func (a *CustomerServerActor) FetchFrame(streamID, readToken string, opts *FrameOptions) ([]byte, error) {
	return a.client.FetchFrame(context.Background(), a.gatewayURL, streamID, readToken, opts)
}

// LastJoinRequest returns the decoded body the control plane last received.
func (a *CustomerServerActor) LastJoinRequest() joinStreamBody {
	return a.controlPlane.lastBody
}

// LastJoinAuthHeader returns the Authorization header the control plane last saw.
func (a *CustomerServerActor) LastJoinAuthHeader() string {
	return a.controlPlane.lastAuth
}

// LastFrameRequest returns the path+query and bearer token the gateway last saw.
func (a *CustomerServerActor) LastFrameRequest() (target, authHeader string) {
	return a.gateway.lastTarget, a.gateway.lastAuth
}

// SetJoinResponse overrides the control plane's reply to the next join request.
func (a *CustomerServerActor) SetJoinResponse(status int, body string) {
	a.controlPlane.status = status
	a.controlPlane.body = body
}

// SetFrameResponse overrides the gateway's reply to the next frame request.
func (a *CustomerServerActor) SetFrameResponse(status int, body []byte) {
	a.gateway.status = status
	a.gateway.body = body
}

// WebhookEndpointActor signs change-trigger deliveries exactly as an Argus
// media server would, then verifies them through the package's ParseWebhook.
type WebhookEndpointActor struct {
	t *testing.T
}

// SignedDelivery returns the body and headers for a webhook signed with secret
// at time ts. A zero-value secret produces an unsigned delivery (no headers).
func (a *WebhookEndpointActor) SignedDelivery(secret string, ts time.Time, frame []byte) ([]byte, http.Header) {
	a.t.Helper()
	body, err := json.Marshal(webhookPayloadWire{
		StreamUUID:  "stream-1",
		Track:       "camera",
		Timestamp:   ts.UTC().Format(time.RFC3339),
		SSIMScore:   0.42,
		FrameFormat: "jpeg",
		FrameBase64: base64.StdEncoding.EncodeToString(frame),
	})
	require.NoError(a.t, err)

	h := http.Header{}
	if secret != "" {
		tsHeader := strconv.FormatInt(ts.Unix(), 10)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(tsHeader))
		mac.Write([]byte("."))
		mac.Write(body)
		h.Set(WebhookSignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
		h.Set(WebhookTimestampHeader, tsHeader)
	}
	return body, h
}

// Parse verifies and decodes a delivery, pinning ParseWebhook's clock to now so
// staleness checks are deterministic.
func (a *WebhookEndpointActor) Parse(secret string, header http.Header, body []byte, now time.Time, opts ...WebhookOption) (*WebhookEvent, error) {
	opts = append(opts, func(o *webhookOptions) { o.now = func() time.Time { return now } })
	return ParseWebhook(secret, header, body, opts...)
}

// --- fakes ------------------------------------------------------------------

// fakeControlPlane stands in for argus-srv's POST /api/streams endpoint. It
// captures the last request and serves a configurable response.
type fakeControlPlane struct {
	status   int
	body     string
	lastBody joinStreamBody
	lastAuth string
}

func newFakeControlPlane() *fakeControlPlane {
	return &fakeControlPlane{
		status: http.StatusCreated,
		body: `{"token":"join-jwt","stream_id":"stream-1",` +
			`"expires_at":"2026-06-30T13:00:00Z","gateway_urls":["https://gw.example.com"],` +
			`"webhook_secret":"` + defaultSecret + `"}`,
	}
}

func (f *fakeControlPlane) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lastAuth = r.Header.Get("Authorization")
	raw, _ := io.ReadAll(r.Body)
	f.lastBody = joinStreamBody{}
	_ = json.Unmarshal(raw, &f.lastBody)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(f.status)
	_, _ = io.WriteString(w, f.body)
}

// fakeGateway stands in for a regional frame gateway's GET /frames/{id}
// endpoint. It captures the last request target and serves configurable bytes.
type fakeGateway struct {
	status     int
	body       []byte
	lastTarget string
	lastAuth   string
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{status: http.StatusOK, body: []byte{0xFF, 0xD8, 0xFF}}
}

func (f *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lastTarget = r.URL.RequestURI()
	f.lastAuth = r.Header.Get("Authorization")
	w.WriteHeader(f.status)
	_, _ = w.Write(f.body)
}
