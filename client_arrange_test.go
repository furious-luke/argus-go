package argus

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Arranger builds named, ready-to-exercise client setups for the specs. Each
// method wires a Client against fake Argus endpoints and returns the actors a
// spec drives. Construction failures are fatal.
type Arranger interface {
	// CustomerServer returns a customer-side client wired to a fake control plane
	// and frame gateway, both seeded with successful default responses.
	CustomerServer() *CustomerServerActor

	// RecoveringCustomerServer is the frame-client world with retry waits made
	// instantaneous while preserving every retry decision and attempt.
	RecoveringCustomerServer() *CustomerServerActor

	// NotifyGateway returns an actor wired to a fake regional gateway that upgrades
	// /notify to a WebSocket and pushes queued notification messages, letting a
	// spec drive Client.Subscribe against it.
	NotifyGateway() *NotifyGatewayActor

	// TLSNotifyGateway returns a notify gateway whose certificate is trusted only
	// by the custom HTTP client supplied through NewWithHTTPClient.
	TLSNotifyGateway() *NotifyGatewayActor

	// HTTP2AdvertisingNotifyGateway returns a notify gateway that genuinely
	// supports HTTP/2, reached through a custom HTTP client whose transport
	// advertises "h2" via ALPN. This reproduces the HTTP/2 gotcha: if the
	// WebSocket dialer inherits that ALPN list, the gateway negotiates HTTP/2
	// and the handshake fails, so the arrangement exercises whether Subscribe
	// still falls back to HTTP/1.1.
	HTTP2AdvertisingNotifyGateway() *NotifyGatewayActor
}

func (a *defaultArranger) RecoveringCustomerServer() *CustomerServerActor {
	a.t.Helper()
	actor := a.CustomerServer()
	originalWait := frameRetryWait
	frameRetryWait = func(ctx context.Context, delay time.Duration) error {
		actor.retryDelays = append(actor.retryDelays, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	a.t.Cleanup(func() { frameRetryWait = originalWait })
	return actor
}

func newArranger(t *testing.T) Arranger {
	return &defaultArranger{t: t}
}

type defaultArranger struct {
	t *testing.T
}

func (a *defaultArranger) CustomerServer() *CustomerServerActor {
	controlPlane := newFakeControlPlane()
	gateway := newFakeGateway()

	cpServer := httptest.NewServer(controlPlane)
	gwServer := httptest.NewServer(gateway)
	a.t.Cleanup(cpServer.Close)
	a.t.Cleanup(gwServer.Close)

	return &CustomerServerActor{
		t:            a.t,
		client:       New(cpServer.URL, defaultAPIKey),
		controlPlane: controlPlane,
		gateway:      gateway,
		gatewayURL:   gwServer.URL,
	}
}

func (a *defaultArranger) NotifyGateway() *NotifyGatewayActor {
	gateway := newFakeNotifyGateway()
	server := httptest.NewServer(gateway)
	a.t.Cleanup(server.Close)

	return &NotifyGatewayActor{
		t:          a.t,
		client:     New("https://control.example", defaultAPIKey),
		gateway:    gateway,
		gatewayURL: server.URL,
	}
}

func (a *defaultArranger) TLSNotifyGateway() *NotifyGatewayActor {
	gateway := newFakeNotifyGateway()
	server := httptest.NewTLSServer(gateway)
	a.t.Cleanup(server.Close)

	return &NotifyGatewayActor{
		t:          a.t,
		client:     NewWithHTTPClient("https://control.example", defaultAPIKey, server.Client()),
		gateway:    gateway,
		gatewayURL: server.URL,
	}
}

func (a *defaultArranger) HTTP2AdvertisingNotifyGateway() *NotifyGatewayActor {
	gateway := newFakeNotifyGateway()
	server := httptest.NewUnstartedServer(gateway)
	server.EnableHTTP2 = true
	server.StartTLS()
	a.t.Cleanup(server.Close)

	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	// NextProtos with "h2" is what Go's transport advertises once HTTP/2 is
	// enabled; carrying it into the WebSocket dial is the bug under test.
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				NextProtos: []string{"h2", "http/1.1"},
			},
		},
	}

	return &NotifyGatewayActor{
		t:          a.t,
		client:     NewWithHTTPClient("https://control.example", defaultAPIKey, httpClient),
		gateway:    gateway,
		gatewayURL: server.URL,
	}
}
