package argus

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyWSURL_NormalizesScheme(t *testing.T) {
	cases := []struct {
		name       string
		gatewayURL string
		wantScheme string
	}{
		{name: "http becomes ws", gatewayURL: "http://gw.example.com", wantScheme: "ws"},
		{name: "https becomes wss", gatewayURL: "https://gw.example.com", wantScheme: "wss"},
		{name: "ws stays ws", gatewayURL: "ws://gw.example.com", wantScheme: "ws"},
		{name: "wss stays wss", gatewayURL: "wss://gw.example.com", wantScheme: "wss"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := notifyWSURL(tc.gatewayURL, "read-jwt", nil)
			require.NoError(t, err)
			u, err := url.Parse(raw)
			require.NoError(t, err)
			assert.Equal(t, tc.wantScheme, u.Scheme)
			assert.Equal(t, "/notify", u.Path)
		})
	}
}

func TestNotifyWSURL_RejectsUnsupportedScheme(t *testing.T) {
	_, err := notifyWSURL("ftp://gw.example.com", "read-jwt", nil)
	require.Error(t, err)
}

func TestNotifyWSURL_AttachesTokenAndWatchParams(t *testing.T) {
	raw, err := notifyWSURL("https://gw.example.com", "read-jwt", &NotifyOptions{
		Track:          "screen",
		Threshold:      0.9,
		PollIntervalMs: 1500,
	})
	require.NoError(t, err)

	u, err := url.Parse(raw)
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "read-jwt", q.Get("token"))
	assert.Equal(t, "screen", q.Get("track"))
	assert.Equal(t, "0.9", q.Get("threshold"))
	assert.Equal(t, "1500", q.Get("poll_interval_ms"))
}

func TestNotifyWSURL_OmitsUnsetWatchParams(t *testing.T) {
	raw, err := notifyWSURL("https://gw.example.com", "read-jwt", nil)
	require.NoError(t, err)

	u, err := url.Parse(raw)
	require.NoError(t, err)
	q := u.Query()
	assert.Equal(t, "read-jwt", q.Get("token"))
	assert.False(t, q.Has("track"))
	assert.False(t, q.Has("threshold"))
	assert.False(t, q.Has("poll_interval_ms"))
}
