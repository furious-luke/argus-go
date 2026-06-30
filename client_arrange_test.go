package argus

import (
	"net/http/httptest"
	"testing"
)

// Arranger builds named, ready-to-exercise client setups for the specs. Each
// method wires a Client against fake Argus endpoints and returns the actors a
// spec drives. Construction failures are fatal.
type Arranger interface {
	// CustomerServer returns a customer-side client wired to a fake control plane
	// and frame gateway, both seeded with successful default responses.
	CustomerServer() *CustomerServerActor

	// WebhookEndpoint returns an actor for signing webhook deliveries the way an
	// Argus media server does and verifying them through ParseWebhook.
	WebhookEndpoint() *WebhookEndpointActor
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

func (a *defaultArranger) WebhookEndpoint() *WebhookEndpointActor {
	return &WebhookEndpointActor{t: a.t}
}
