package client

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const testSecret = "whsec-client-test"

// signedDelivery builds a body + headers exactly as the media server would.
func signedDelivery(t *testing.T, secret string, ts time.Time, frame []byte) ([]byte, http.Header) {
	t.Helper()
	body, err := json.Marshal(webhookPayloadWire{
		StreamUUID:  "stream-1",
		Track:       "camera",
		Timestamp:   ts.UTC().Format(time.RFC3339),
		SSIMScore:   0.42,
		FrameFormat: "jpeg",
		FrameBase64: base64.StdEncoding.EncodeToString(frame),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

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

func TestParseWebhook_ValidSignatureDecodesFrame(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	frame := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x02}
	body, h := signedDelivery(t, testSecret, now, frame)

	ev, err := ParseWebhook(testSecret, h, body, WithTolerance(time.Minute), func(o *webhookOptions) { o.now = func() time.Time { return now } })
	if err != nil {
		t.Fatalf("ParseWebhook: %v", err)
	}
	if ev.StreamID != "stream-1" {
		t.Errorf("stream id = %q", ev.StreamID)
	}
	if ev.Track != "camera" {
		t.Errorf("track = %q", ev.Track)
	}
	if ev.SSIMScore != 0.42 {
		t.Errorf("ssim = %v", ev.SSIMScore)
	}
	if string(ev.Frame) != string(frame) {
		t.Errorf("frame mismatch: got %v", ev.Frame)
	}
	if !ev.Timestamp.Equal(now) {
		t.Errorf("timestamp = %v, want %v", ev.Timestamp, now)
	}
}

func TestParseWebhook_RejectsForgedAndMissing(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	frame := []byte{0x01}

	cases := []struct {
		name    string
		mutate  func(body []byte, h http.Header) (string, []byte, http.Header)
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
				return testSecret, append(b, ' '), h
			},
			wantErr: ErrInvalidSignature,
		},
		{
			name: "missing signature header",
			mutate: func(b []byte, h http.Header) (string, []byte, http.Header) {
				h.Del(WebhookSignatureHeader)
				return testSecret, b, h
			},
			wantErr: ErrMissingSignature,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, h := signedDelivery(t, testSecret, now, frame)
			secret, body, h := tc.mutate(body, h)
			_, err := ParseWebhook(secret, h, body, func(o *webhookOptions) { o.now = func() time.Time { return now } })
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseWebhook_RejectsStaleTimestamp(t *testing.T) {
	signedAt := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	body, h := signedDelivery(t, testSecret, signedAt, []byte{0x01})

	// Verify 10 minutes later with a 5-minute tolerance.
	later := signedAt.Add(10 * time.Minute)
	_, err := ParseWebhook(testSecret, h, body, WithTolerance(5*time.Minute), func(o *webhookOptions) { o.now = func() time.Time { return later } })
	if !errors.Is(err, ErrStaleWebhook) {
		t.Fatalf("err = %v, want ErrStaleWebhook", err)
	}
}

func TestParseWebhook_NoSecretSkipsVerification(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	// Body with no signature headers at all.
	body, _ := signedDelivery(t, "", now, []byte{0x09})

	ev, err := ParseWebhook("", http.Header{}, body)
	if err != nil {
		t.Fatalf("ParseWebhook: %v", err)
	}
	if len(ev.Frame) != 1 || ev.Frame[0] != 0x09 {
		t.Errorf("frame mismatch: %v", ev.Frame)
	}
}

func TestParseWebhook_MalformedBody(t *testing.T) {
	_, err := ParseWebhook("", http.Header{}, []byte("not json"))
	if !errors.Is(err, ErrMalformedWebhook) {
		t.Fatalf("err = %v, want ErrMalformedWebhook", err)
	}
}
