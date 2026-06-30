package client

import (
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

func TestSpec_Join_ForwardsRegionAndTrigger(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(&JoinOptions{
		Region:  "eu-west-1",
		Trigger: &TriggerConfig{WebhookURL: "https://example.com/webhook"},
	})
	sent := server.LastJoinRequest()
	assert.Equal(t, "eu-west-1", sent.Region)
	require.NotNil(t, sent.Trigger)
	assert.Equal(t, "https://example.com/webhook", sent.Trigger.WebhookURL)
}

func TestSpec_Join_OmitsTriggerWhenUnset(t *testing.T) {
	a := newArranger(t)
	server := a.CustomerServer()
	server.MustJoin(&JoinOptions{Region: "us-east-1"})
	assert.Nil(t, server.LastJoinRequest().Trigger)
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

func TestSpec_Webhook_ValidSignatureDecodesFrame(t *testing.T) {
	a := newArranger(t)
	endpoint := a.WebhookEndpoint()
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	frame := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x02}
	body, h := endpoint.SignedDelivery(defaultSecret, now, frame)

	ev, err := endpoint.Parse(defaultSecret, h, body, now, WithTolerance(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, "stream-1", ev.StreamID)
	assert.Equal(t, "camera", ev.Track)
	assert.Equal(t, 0.42, ev.SSIMScore)
	assert.Equal(t, frame, ev.Frame)
	assert.True(t, ev.Timestamp.Equal(now))
}

func TestSpec_Webhook_RejectsForgedAndMissing(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		mutate  func(body []byte, h http.Header) (secret string, outBody []byte, outHeader http.Header)
		wantErr error
	}{
		{
			name: "wrong secret",
			mutate: func(b []byte, h http.Header) (string, []byte, http.Header) {
				return "different-secret", b, h
			},
			wantErr: ErrInvalidSignature,
		},
		{
			name: "tampered body",
			mutate: func(b []byte, h http.Header) (string, []byte, http.Header) {
				return defaultSecret, append(b, ' '), h
			},
			wantErr: ErrInvalidSignature,
		},
		{
			name: "missing signature header",
			mutate: func(b []byte, h http.Header) (string, []byte, http.Header) {
				h.Del(WebhookSignatureHeader)
				return defaultSecret, b, h
			},
			wantErr: ErrMissingSignature,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newArranger(t)
			endpoint := a.WebhookEndpoint()
			body, h := endpoint.SignedDelivery(defaultSecret, now, []byte{0x01})
			secret, body, h := tc.mutate(body, h)
			_, err := endpoint.Parse(secret, h, body, now)
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestSpec_Webhook_RejectsStaleTimestamp(t *testing.T) {
	a := newArranger(t)
	endpoint := a.WebhookEndpoint()
	signedAt := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	body, h := endpoint.SignedDelivery(defaultSecret, signedAt, []byte{0x01})

	// Verify 10 minutes later with a 5-minute tolerance.
	later := signedAt.Add(10 * time.Minute)
	_, err := endpoint.Parse(defaultSecret, h, body, later, WithTolerance(5*time.Minute))
	assert.ErrorIs(t, err, ErrStaleWebhook)
}

func TestSpec_Webhook_NoSecretSkipsVerification(t *testing.T) {
	a := newArranger(t)
	endpoint := a.WebhookEndpoint()
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	// An unsigned delivery with no signature headers at all.
	body, _ := endpoint.SignedDelivery("", now, []byte{0x09})

	ev, err := endpoint.Parse("", http.Header{}, body, now)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x09}, ev.Frame)
}

func TestSpec_Webhook_RejectsMalformedBody(t *testing.T) {
	a := newArranger(t)
	endpoint := a.WebhookEndpoint()
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	_, err := endpoint.Parse("", http.Header{}, []byte("not json"), now)
	assert.ErrorIs(t, err, ErrMalformedWebhook)
}
