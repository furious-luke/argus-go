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
	assert.Equal(t, "control-jwt", join.ControlToken)
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

func TestSpec_Join_ForwardsRecordingOptions(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(&JoinOptions{
		RecordingEnabled:       true,
		RecordingRetentionDays: 30,
		StorageRegion:          "eu-central-1",
	})
	req := server.LastJoinRequest()
	assert.True(t, req.RecordingEnabled, "recording is enabled on the create request")
	assert.Equal(t, 30, req.RecordingRetentionDays)
	assert.Equal(t, "eu-central-1", req.StorageRegion, "storage-at-rest region is pinned independently of the processing region")
}

func TestSpec_Join_ForwardsServerSelectedVoiceConfiguration(t *testing.T) {
	server := newArranger(t).CustomerServer()
	server.MustJoin(&JoinOptions{Voice: &VoiceConfig{CatalogVersion: "2026-08-31.1", ByProvider: map[string]VoiceProviderRef{"google": {VoiceID: "Aoede"}}}})

	assert.Equal(t, "Aoede", server.LastJoinRequest().Voice.ByProvider["google"].VoiceID)
}

func TestSpec_Voices_DiscoversTheCurrentCatalogue(t *testing.T) {
	server := newArranger(t).CustomerServer()
	catalog := server.MustGetVoices()

	assert.Equal(t, "Aoede", catalog.Providers[0].Voices[0].ID)
}

func TestSpec_Voices_AuthenticatesDiscoveryWithTheAPIKey(t *testing.T) {
	server := newArranger(t).CustomerServer()
	server.MustGetVoices()

	assert.Equal(t, "ApiKey "+defaultAPIKey, server.LastVoiceCatalogAuthHeader())
}

// Recording is off and no storage region is pinned unless explicitly requested,
// so the fields are omitted from the create request by default.
func TestSpec_Join_OmitsRecordingByDefault(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(&JoinOptions{Region: "eu-west-1"})
	req := server.LastJoinRequest()
	assert.False(t, req.RecordingEnabled)
	assert.Zero(t, req.RecordingRetentionDays)
	assert.Empty(t, req.StorageRegion)
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
	server.MustFetchFrame("stream-1", "read-jwt", &FrameOptions{Track: TrackScreen, Format: "png"})
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

func TestSpec_Subscribe_AuthenticatesWithControlTokenAndAttachesWatchParams(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, &NotifyOptions{Track: TrackScreen, Threshold: 0.9, PollIntervalMs: 1500})
	require.NoError(t, err)

	target := gateway.LastConnectTarget()
	assert.Contains(t, target, "/notify")
	assert.NotContains(t, target, "token=")
	assert.Equal(t, "Bearer control-jwt", gateway.LastAuthorization())
	assert.Contains(t, target, "track=screen")
	assert.Contains(t, target, "threshold=0.9")
	assert.Contains(t, target, "poll_interval_ms=1500")
}

func TestSpec_NotifySubscription_StreamsIdentifiedUtteranceCommands(t *testing.T) {
	gateway := newArranger(t).NotifyGateway()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	messages := gateway.StreamUtterance(ctx, "u1", "hello ", "world")
	require.Len(t, messages, 4)
	assert.Equal(t, notifyMsgUtteranceStart, messages[0].Type)
	assert.Equal(t, "u1", messages[0].UtteranceID)
	assert.Equal(t, []string{"hello ", "world"}, []string{messages[1].Text, messages[2].Text})
	assert.Equal(t, notifyMsgUtteranceEnd, messages[3].Type)
}

func TestSpec_NotifySubscription_StalledCommandWriteReturnsWithinTransportBound(t *testing.T) {
	gateway := newArranger(t).NotifyGateway()
	assert.True(t, gateway.StalledCommandReturnsWithinBound())
}

// Context cancellation interrupts a blocked command through the socket-close
// path; it never becomes a second writer by changing the write deadline.
func TestSpec_NotifySubscription_CancellationPreservesSingleTransportWriter(t *testing.T) {
	gateway := newArranger(t).NotifyGateway()
	assert.True(t, gateway.CancellationKeepsSingleWriter())
}

// A transcript-only customer owns the notify subscription but explicitly tells
// the server not to allocate its video watcher.
func TestSpec_Subscribe_OmitsVideoWorkWithoutFrameHandler(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.SubscribeTranscriptsOnly(ctx))

	assert.Contains(t, gateway.LastConnectTarget(), "watch_frames=false")
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

// A customer HTTP client configured for HTTP/2 advertises "h2" via ALPN. The
// WebSocket handshake runs over HTTP/1.1, so the client must not let that ALPN
// list reach the notify dial — otherwise the gateway negotiates HTTP/2 and the
// handshake dies on an unparseable HTTP/2 frame ("protocol \"h2\" was given but
// is not supported"). Subscribe succeeding proves it negotiated HTTP/1.1.
func TestSpec_Subscribe_SucceedsWhenHTTPClientAdvertisesHTTP2(t *testing.T) {
	a := newArranger(t)
	gateway := a.HTTP2AdvertisingNotifyGateway()
	gateway.EnqueueStreamEnded()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gateway.Subscribe(ctx, nil)
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

func TestSpec_Subscribe_ReportsSpeechStarted(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueSpeechStarted()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))
	assert.Equal(t, 1, gateway.SpeechStartedCount())
}

func TestSpec_Subscribe_DeliversFinalTranscriptText(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueTranscript("turn left at the second light")
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))
	assert.Equal(t, []string{"turn left at the second light"}, gateway.Transcripts())
}

func TestSpec_Subscribe_ReportsNoSpeech(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueNoSpeech()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))
	assert.Equal(t, 1, gateway.NoSpeechCount())
}

func TestSpec_Subscribe_ReportsRecoverableTranscriptionInterruption(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueTranscriptionInterrupted()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))
	assert.Equal(t, 1, gateway.TranscriptionInterruptedCount())
}

func TestSpec_Subscribe_ReportsTerminalTranscriptionUnavailability(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueTranscriptionUnavailable()
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))
	assert.Equal(t, 1, gateway.TranscriptionUnavailableCount())
}

func TestSpec_Subscribe_DeliversUserTextWithMessageIdentity(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueUserText("msg-7", "can you repeat that")
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))

	texts := gateway.UserTexts()
	require.Len(t, texts, 1)
	assert.Equal(t, "msg-7", texts[0].MessageID)
	assert.Equal(t, "can you repeat that", texts[0].Text)
}

func TestSpec_Subscribe_SurfacesUtteranceLifecycleFields(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	complete := true
	gateway.EnqueueUtteranceEvent(notifyMsgUtteranceFinished, "u1", "streaming", "", &complete)
	gateway.EnqueueUtteranceEvent(notifyMsgUtteranceRejected, "u2", "", "stream not accepting speech", nil)
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))

	events := gateway.UtteranceEvents()
	require.Len(t, events, 2)
	assert.Equal(t, UtteranceEvent{Type: notifyMsgUtteranceFinished, UtteranceID: "u1", DeliveryMode: "streaming", TextComplete: true}, events[0])
	assert.Equal(t, UtteranceEvent{Type: notifyMsgUtteranceRejected, UtteranceID: "u2", Reason: "stream not accepting speech"}, events[1])
}

func TestSpec_Subscribe_SurfacesEveryUtteranceLifecycleType(t *testing.T) {
	cases := []struct {
		name    string
		msgType string
	}{
		{"queued", notifyMsgUtteranceQueued},
		{"started", notifyMsgUtteranceStarted},
		{"paused", notifyMsgUtterancePaused},
		{"resumed", notifyMsgUtteranceResumed},
		{"finished", notifyMsgUtteranceFinished},
		{"cancelled", notifyMsgUtteranceCancelled},
		{"failed", notifyMsgUtteranceFailed},
		{"rejected", notifyMsgUtteranceRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newArranger(t)
			gateway := a.NotifyGateway()
			gateway.EnqueueUtteranceEvent(tc.msgType, "u1", "", "", nil)
			gateway.EnqueueStreamEnded()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, gateway.Subscribe(ctx, nil))

			events := gateway.UtteranceEvents()
			require.Len(t, events, 1)
			assert.Equal(t, tc.msgType, events[0].Type)
			assert.Equal(t, "u1", events[0].UtteranceID)
		})
	}
}

func TestSpec_Subscribe_OmitsTextCompleteWhenAbsentFromWire(t *testing.T) {
	a := newArranger(t)
	gateway := a.NotifyGateway()
	gateway.EnqueueUtteranceEvent(notifyMsgUtteranceFinished, "u1", "buffered", "", nil)
	gateway.EnqueueStreamEnded()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, gateway.Subscribe(ctx, nil))

	events := gateway.UtteranceEvents()
	require.Len(t, events, 1)
	assert.False(t, events[0].TextComplete)
}

func notifyFrameBytes(events []NotifyEvent) [][]byte {
	frames := make([][]byte, len(events))
	for i, event := range events {
		frames[i] = event.Frame
	}
	return frames
}
