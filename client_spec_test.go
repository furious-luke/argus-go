package argus

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These specs describe the observable behaviour of the Argus client library
// from a customer server's point of view. They are human-owned contracts: they
// say what the client does, not how it is built. See client_arrange_test.go and
// client_actor_test.go for the supporting harness.

func TestSpec_Join_ReturnsTokenBundle(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	join := server.MustJoin(nil)
	assert.Equal(t, "join-jwt", join.Token)
	assert.Equal(t, "stream-1", join.StreamID)
	assert.Equal(t, []string{"https://gw.example.com"}, join.GatewayURLs)
}

func TestSpec_Join_AuthenticatesWithAPIKey(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(nil)
	assert.Equal(t, "ApiKey "+defaultAPIKey, server.LastJoinAuthHeader())
}

func TestSpec_Join_ForwardsRegion(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(&JoinOptions{Region: "eu-west-1"})
	assert.Equal(t, "eu-west-1", server.LastJoinRequest().Region)
}

func TestSpec_Join_SurfacesServerError(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.SetJoinResponse(http.StatusForbidden, "region violates residency policy")
	_, err := server.Join(&JoinOptions{Region: "us-east-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region violates residency policy")
}

func TestSpec_Frame_ReturnsImageBytes(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.SetFrameResponse(http.StatusOK, []byte{0x89, 0x50, 0x4E, 0x47})
	frame := server.MustFetchFrame("stream-1", "read-jwt", nil)
	assert.Equal(t, []byte{0x89, 0x50, 0x4E, 0x47}, frame)
}

func TestSpec_Frame_DefaultsTrackAndFormat(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustFetchFrame("stream-1", "read-jwt", nil)
	target, _ := server.LastFrameRequest()
	assert.Contains(t, target, "track=camera")
	assert.Contains(t, target, "format=jpeg")
}

func TestSpec_Frame_HonoursTrackAndFormat(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustFetchFrame("stream-1", "read-jwt", &FrameOptions{Track: "screen", Format: "png"})
	target, _ := server.LastFrameRequest()
	assert.Contains(t, target, "track=screen")
	assert.Contains(t, target, "format=png")
}

func TestSpec_Frame_AcceptsJoinResponseSignalingURL(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.UseSignalingGatewayURL()
	server.MustFetchFrame("stream-1", "read-jwt", nil)
	target, _ := server.LastFrameRequest()
	assert.Contains(t, target, "/frames/stream-1")
}

func TestSpec_Frame_AuthenticatesWithReadToken(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustFetchFrame("stream-1", "read-jwt", nil)
	_, auth := server.LastFrameRequest()
	assert.Equal(t, "Bearer read-jwt", auth)
}

func TestSpec_Frame_SurfacesServerError(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.SetFrameResponse(http.StatusNotFound, []byte("stream not found"))
	_, err := server.FetchFrame("stream-1", "read-jwt", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream not found")
}

func TestSpec_Frame_TransientStallRecoversWithoutSurfacingOldFrameOrError(t *testing.T) {
	a := newArranger(t)
	server := a.RecoveringCustomerServer()
	freshFrame := []byte("fresh-frame")
	server.MustGatewayRecoverAfter(2, 17*time.Second, freshFrame)
	frame := server.MustFetchFrame("stream-1", "read-jwt", nil)
	assert.Equal(t, freshFrame, frame)
	assert.Equal(t, 3, server.FrameRequestCount())
}

func TestSpec_Frame_PersistentStallReturnsTypedErrorAfterBoundedRecovery(t *testing.T) {
	a := newArranger(t)
	server := a.RecoveringCustomerServer()
	server.MustGatewayRemainStalled(23*time.Second, time.Second)
	_, err := server.FetchFrame("stream-1", "read-jwt", nil)
	var stale *StaleFrameError
	require.ErrorAs(t, err, &stale)
	assert.ErrorIs(t, err, ErrStaleFrame)
	assert.Equal(t, 23*time.Second, stale.FrameAge)
	assert.Equal(t, 5, server.FrameRequestCount())
}

func TestSpec_Frame_OnlyAuthoritativeStaleResponseIsRetried(t *testing.T) {
	a := newArranger(t)
	server := a.RecoveringCustomerServer()
	server.MustGatewayReturnUnmarkedUnavailable()
	_, err := server.FetchFrame("stream-1", "read-jwt", nil)
	assert.NotErrorIs(t, err, ErrStaleFrame)
	assert.Equal(t, 1, server.FrameRequestCount())
}

func TestSpec_Frame_RetryAdviceIsBoundedBeforeUse(t *testing.T) {
	a := newArranger(t)
	server := a.RecoveringCustomerServer()
	server.MustGatewayRemainStalled(20*time.Second, 30*time.Second)
	server.FetchFrame("stream-1", "read-jwt", nil)
	assert.Equal(t, []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second, 2 * time.Second}, server.RetryDelays())
}

func TestSpec_Subscribe_DeliversFrameBytes(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	frame := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x02}
	gateway.EnqueueFrame("camera", 0.42, now, frame)
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
	require.NoError(t, err)

	frames := gateway.Frames()
	require.Len(t, frames, 1)
	assert.Equal(t, "stream-1", frames[0].StreamID)
	assert.Equal(t, "camera", frames[0].Track)
	assert.Equal(t, 0.42, frames[0].SSIMScore)
	assert.Equal(t, "jpeg", frames[0].FrameFormat)
	assert.Equal(t, frame, frames[0].Frame)
	assert.True(t, frames[0].Timestamp.Equal(now))
}

func TestSpec_Subscribe_AttachesTokenAndWatchParams(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, &NotifyOptions{Track: "screen", Threshold: 0.9, PollIntervalMs: 1500})
	require.NoError(t, err)

	target := gateway.LastConnectTarget()
	assert.Contains(t, target, "/notify")
	assert.Contains(t, target, "token=read-jwt")
	assert.Contains(t, target, "track=screen")
	assert.Contains(t, target, "threshold=0.9")
	assert.Contains(t, target, "poll_interval_ms=1500")
}

func TestSpec_Subscribe_ReturnsWhenStreamEnds(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "stream_ended", gateway.EndReason())
}

func TestSpec_Subscribe_ReturnsWhenSuperseded(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueSuperseded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "superseded", gateway.EndReason())
}

func TestSpec_Subscribe_SurfacesErrorMessage(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueError("token expired")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token expired")
}

func TestSpec_Subscribe_ReportsTokenExpiring(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueTokenExpiring()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, gateway.TokenExpiringCount())
}

func TestSpec_Subscribe_ReconnectsAfterEstablishedSocketDrops(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.DisconnectFirstConnectionAfter([]byte("before"), []byte("after"))
	err := gateway.Subscribe(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("before"), []byte("after")}, notifyFrameBytes(gateway.Frames()))
	assert.Equal(t, 2, gateway.ConnectionCount())
}

func TestSpec_Subscribe_RejectsMismatchedStreamEnvelope(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueFrameForStream("another-stream", "camera", 0.5, time.Now(), []byte("frame"))
	err := gateway.Subscribe(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stream mismatch")
	assert.Empty(t, gateway.Frames())
}

func TestSpec_Subscribe_SurfacesHandshakeStatusAndBody(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.RejectHandshake(http.StatusUnauthorized, "token revoked")
	err := gateway.Subscribe(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Contains(t, err.Error(), "token revoked")
}

func TestSpec_Subscribe_UsesCustomTLSConfiguration(t *testing.T) {
	a := newArranger(t)
	gateway := a.TLSNotifyGateway()
	gateway.EnqueueStreamEnded()
	err := gateway.Subscribe(context.Background(), nil)
	require.NoError(t, err)
}

func TestSpec_Subscribe_ReturnsWhenContextCancelled(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	// No terminal message queued: the gateway holds the socket open, so Subscribe
	// blocks on the read until the context is cancelled.

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gateway.Subscribe(ctx, nil) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}

// A subscription that ends on its own — without the caller cancelling the
// context — must not leak the socket-closing watcher goroutine. This is the
// common case: the camera demo passes context.Background(), which is never
// cancelled, so a leaked watcher per completed subscription would accumulate
// indefinitely.
func TestSpec_Subscribe_DoesNotLeakGoroutineWhenStreamEnds(t *testing.T) {
	a := newArranger(t)
	// A single gateway (one httptest server) reused across every run, so the only
	// thing that could grow with the run count is the client's own watcher
	// goroutine — not per-gateway harness goroutines.
	gateway := a.NotifyGateway()
	gateway.EnqueueStreamEnded()

	// Run many self-ending subscriptions against a never-cancelled context (the
	// camera demo's exact usage). If the watcher goroutine were tied only to the
	// caller's context, each completed subscription would strand one goroutine on
	// <-ctx.Done().
	const runs = 50
	before := goroutineCount()
	for range runs {
		require.NoError(t, gateway.Subscribe(context.Background(), nil))
	}

	// Allow the (correctly cancelled) watchers to unwind, then assert the count
	// settled back near the baseline rather than growing by ~runs.
	if !eventually(2*time.Second, func() bool {
		return goroutineCount() <= before+5
	}) {
		t.Fatalf("goroutine count grew from %d to %d across %d completed subscriptions: watcher goroutine leaked", before, goroutineCount(), runs)
	}
}

func notifyFrameBytes(events []NotifyEvent) [][]byte {
	frames := make([][]byte, len(events))
	for i, event := range events {
		frames[i] = event.Frame
	}
	return frames
}
