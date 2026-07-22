package argus

import (
	"fmt"
	"net/url"
)

type gatewayTransport int

const (
	gatewayHTTP gatewayTransport = iota
	gatewayWebSocket
)

// gatewayBaseURL accepts either the signaling URL returned by JoinStream or an
// already-normalized gateway base URL. Argus gateway services live at the
// origin, so signaling paths and their query/fragment are deliberately removed.
func gatewayBaseURL(raw string, transport gatewayTransport) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse gateway url: %w", err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("gateway url has no host")
	}

	switch transport {
	case gatewayHTTP:
		switch u.Scheme {
		case "http", "https":
		case "ws":
			u.Scheme = "http"
		case "wss":
			u.Scheme = "https"
		default:
			return nil, fmt.Errorf("unsupported gateway scheme %q", u.Scheme)
		}
	case gatewayWebSocket:
		switch u.Scheme {
		case "http":
			u.Scheme = "ws"
		case "https":
			u.Scheme = "wss"
		case "ws", "wss":
		default:
			return nil, fmt.Errorf("unsupported gateway scheme %q", u.Scheme)
		}
	default:
		return nil, fmt.Errorf("unsupported gateway transport")
	}

	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}
