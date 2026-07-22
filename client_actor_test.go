package argus

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const defaultAPIKey = "argus_api_key_test"

// CustomerServerActor drives a Client against a fake control plane and frame
// gateway, standing in for a customer's own backend. The captured requests on
// the embedded fakes let specs assert on what the client actually sent.
type CustomerServerActor struct {
	t            *testing.T
	client       *Client
	controlPlane *fakeControlPlane
	gateway      *fakeGateway
	gatewayURL   string
	retryDelays  []time.Duration
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

// UseSignalingGatewayURL changes the frame endpoint input to the ws://.../signal
// shape returned by JoinStream, including irrelevant query and fragment data.
func (a *CustomerServerActor) UseSignalingGatewayURL() {
	a.gatewayURL = "ws" + strings.TrimPrefix(a.gatewayURL, "http") + "/signal?candidate=one#ignored"
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
	a.gateway.headers = make(http.Header)
	a.gateway.responses = nil
}

func (a *CustomerServerActor) MustGatewayRecoverAfter(staleResponses int, age time.Duration, frame []byte) {
	a.t.Helper()
	responses := make([]fakeFrameResponse, 0, staleResponses+1)
	for range staleResponses {
		responses = append(responses, staleFakeFrameResponse(age, time.Second))
	}
	responses = append(responses, fakeFrameResponse{status: http.StatusOK, body: append([]byte(nil), frame...), headers: make(http.Header)})
	a.gateway.responses = responses
}

func (a *CustomerServerActor) MustGatewayRemainStalled(age, retryAfter time.Duration) {
	a.t.Helper()
	response := staleFakeFrameResponse(age, retryAfter)
	a.gateway.status = response.status
	a.gateway.body = response.body
	a.gateway.headers = response.headers
	a.gateway.responses = nil
}

func (a *CustomerServerActor) MustGatewayReturnUnmarkedUnavailable() {
	a.t.Helper()
	a.gateway.status = http.StatusServiceUnavailable
	a.gateway.body = []byte("temporarily unavailable")
	a.gateway.headers = make(http.Header)
	a.gateway.responses = nil
}

func (a *CustomerServerActor) FrameRequestCount() int {
	a.t.Helper()
	return a.gateway.attempts
}

func (a *CustomerServerActor) RetryDelays() []time.Duration {
	a.t.Helper()
	return append([]time.Duration(nil), a.retryDelays...)
}

// NotifyGatewayActor drives Client.Subscribe against a fake regional gateway
// that upgrades /notify to a WebSocket. A spec queues the messages the gateway
// should push, runs Subscribe (collecting frames and the end reason), and then
// asserts on what the client observed and on the connect target the gateway saw.
type NotifyGatewayActor struct {
	t          *testing.T
	client     *Client
	gateway    *fakeNotifyGateway
	gatewayURL string

	mu        sync.Mutex
	frames    []NotifyEvent
	endReason string
	expiring  int
}

// EnqueueFrame queues a frame message the gateway pushes on connect.
func (a *NotifyGatewayActor) EnqueueFrame(track string, ssim float64, ts time.Time, frame []byte) {
	a.EnqueueFrameForStream("stream-1", track, ssim, ts, frame)
}

// EnqueueFrameForStream queues a frame carrying an explicit authenticated stream
// identity, including a deliberately wrong one for mismatch contracts.
func (a *NotifyGatewayActor) EnqueueFrameForStream(streamID, track string, ssim float64, ts time.Time, frame []byte) {
	a.gateway.enqueue(notifyWire{
		Type:        notifyMsgFrame,
		Stream:      streamID,
		Track:       track,
		SSIMScore:   ssim,
		FrameFormat: "jpeg",
		FrameBase64: base64.StdEncoding.EncodeToString(frame),
		Timestamp:   ts.UTC().Format(time.RFC3339),
	})
}

// DisconnectFirstConnectionAfter arranges a transient customer-hop loss after
// the first frame. The next connection receives second and then ends normally.
func (a *NotifyGatewayActor) DisconnectFirstConnectionAfter(first, second []byte) {
	now := time.Date(2026, 7, 22, 1, 2, 3, 0, time.UTC)
	a.gateway.setScripts([][]notifyWire{
		{{
			Type:        notifyMsgFrame,
			Stream:      "stream-1",
			Track:       "camera",
			FrameFormat: "jpeg",
			FrameBase64: base64.StdEncoding.EncodeToString(first),
			Timestamp:   now.Format(time.RFC3339),
		}},
		{
			{
				Type:        notifyMsgFrame,
				Stream:      "stream-1",
				Track:       "camera",
				FrameFormat: "jpeg",
				FrameBase64: base64.StdEncoding.EncodeToString(second),
				Timestamp:   now.Add(time.Second).Format(time.RFC3339),
			},
			{Type: notifyMsgStreamEnded, Stream: "stream-1"},
		},
	})
}

// RejectHandshake makes the fake return an HTTP error before WebSocket upgrade.
func (a *NotifyGatewayActor) RejectHandshake(status int, body string) {
	a.gateway.reject(status, body)
}

// EnqueueTokenExpiring queues a token_expiring message.
func (a *NotifyGatewayActor) EnqueueTokenExpiring() {
	a.gateway.enqueue(notifyWire{Type: notifyMsgTokenExpiring})
}

// EnqueueStreamEnded queues a stream_ended message.
func (a *NotifyGatewayActor) EnqueueStreamEnded() {
	a.gateway.enqueue(notifyWire{Type: notifyMsgStreamEnded})
}

// EnqueueSuperseded queues a superseded message.
func (a *NotifyGatewayActor) EnqueueSuperseded() {
	a.gateway.enqueue(notifyWire{Type: notifyMsgSuperseded})
}

// EnqueueError queues an error message carrying reason.
func (a *NotifyGatewayActor) EnqueueError(reason string) {
	a.gateway.enqueue(notifyWire{Type: notifyMsgError, Reason: reason})
}

// handlers records everything the subscription observes so specs can assert on
// it after Subscribe returns.
func (a *NotifyGatewayActor) handlers() NotifyHandlers {
	return NotifyHandlers{
		OnFrame: func(ev NotifyEvent) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.frames = append(a.frames, ev)
		},
		OnTokenExpiring: func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.expiring++
		},
		OnEnded: func(reason string) {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.endReason = reason
		},
	}
}

// Subscribe runs Client.Subscribe against the fake gateway and returns its error
// for the spec to assert on.
func (a *NotifyGatewayActor) Subscribe(ctx context.Context, opts *NotifyOptions) error {
	return a.client.Subscribe(ctx, a.gatewayURL, "stream-1", "read-jwt", opts, a.handlers())
}

// Frames returns the frames the subscription delivered, in order.
func (a *NotifyGatewayActor) Frames() []NotifyEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]NotifyEvent(nil), a.frames...)
}

// EndReason returns the reason OnEnded reported, or "" if it never fired.
func (a *NotifyGatewayActor) EndReason() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.endReason
}

// TokenExpiringCount returns how many times OnTokenExpiring fired.
func (a *NotifyGatewayActor) TokenExpiringCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.expiring
}

// LastConnectTarget returns the path+query the gateway saw on the WebSocket
// upgrade request.
func (a *NotifyGatewayActor) LastConnectTarget() string {
	return a.gateway.connectTarget()
}

func (a *NotifyGatewayActor) ConnectionCount() int {
	return a.gateway.connectionCount()
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
			`"expires_at":"2026-06-30T13:00:00Z","gateway_urls":["https://gw.example.com"]}`,
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
	headers    http.Header
	responses  []fakeFrameResponse
	attempts   int
	lastTarget string
	lastAuth   string
}

type fakeFrameResponse struct {
	status  int
	body    []byte
	headers http.Header
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{status: http.StatusOK, body: []byte{0xFF, 0xD8, 0xFF}, headers: make(http.Header)}
}

func (f *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.attempts++
	f.lastTarget = r.URL.RequestURI()
	f.lastAuth = r.Header.Get("Authorization")
	response := fakeFrameResponse{status: f.status, body: f.body, headers: f.headers}
	if len(f.responses) > 0 {
		response = f.responses[0]
		f.responses = f.responses[1:]
	}
	for name, values := range response.headers {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}

func staleFakeFrameResponse(age, retryAfter time.Duration) fakeFrameResponse {
	headers := make(http.Header)
	headers.Set(frameAgeHeader, strconv.FormatInt(age.Milliseconds(), 10))
	headers.Set("Retry-After", strconv.FormatInt(int64(retryAfter/time.Second), 10))
	return fakeFrameResponse{status: http.StatusServiceUnavailable, body: []byte("frame is stale"), headers: headers}
}

// fakeNotifyGateway stands in for a regional gateway's GET /notify endpoint. It
// upgrades the connection to a WebSocket, pushes every queued message in order,
// and then blocks until the peer (or the test) closes the socket.
type fakeNotifyGateway struct {
	upgrader websocket.Upgrader
	messages []notifyWire
	scripts  [][]notifyWire
	status   int
	body     string

	mu          sync.Mutex
	target      string
	connections int
}

func newFakeNotifyGateway() *fakeNotifyGateway {
	return &fakeNotifyGateway{}
}

func (f *fakeNotifyGateway) enqueue(msg notifyWire) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.messages = append(f.messages, msg)
}

func (f *fakeNotifyGateway) setScripts(scripts [][]notifyWire) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scripts = scripts
}

func (f *fakeNotifyGateway) reject(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
	f.body = body
}

func (f *fakeNotifyGateway) connectTarget() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target
}

func (f *fakeNotifyGateway) connectionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connections
}

func (f *fakeNotifyGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.target = r.URL.RequestURI()
	if f.status != 0 {
		status, body := f.status, f.body
		f.mu.Unlock()
		http.Error(w, body, status)
		return
	}
	connection := f.connections
	f.connections++
	messages := append([]notifyWire(nil), f.messages...)
	scripted := len(f.scripts) > 0
	if scripted {
		index := min(connection, len(f.scripts)-1)
		messages = append([]notifyWire(nil), f.scripts[index]...)
	}
	f.mu.Unlock()

	conn, err := f.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	for _, msg := range messages {
		if err := conn.WriteJSON(msg); err != nil {
			return
		}
	}
	if scripted {
		return
	}

	// Keep the socket open until the client closes it (e.g. on context cancel or
	// after a terminal message). ReadMessage returns once the peer goes away.
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// goroutineCount returns the current number of goroutines. Used by leak checks.
func goroutineCount() int {
	return runtime.NumGoroutine()
}

// eventually polls cond until it is true or the timeout elapses, so goroutine
// teardown (which is asynchronous) has time to settle before a leak assertion.
func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}
