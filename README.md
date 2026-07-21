# Argus Go Client

A small, dependency-free Go client for the Argus video streaming platform. It runs on a customer's own backend and wraps the three things a
server-side integration needs:

- **Mint join tokens** for new streams, to hand to an end user's browser.
- **Fetch decoded frames** from a regional frame gateway.
- **Verify and decode change-trigger webhooks** delivered by Argus.

The package imports only the Go standard library, so it can be vendored or
dropped into an external application without pulling in the rest of Argus.

## Background

Argus is a regionally distributed video ingestion service. End users stream
video from their browsers to Argus media servers, which hold the most recent
frames in memory; application servers — typically powering AI agents — fetch a
still frame on demand whenever the agent needs to "see". Three parties are
involved:

- **Argus** operates the platform (control plane + regional gateways and media
  fleets).
- **Customers** are businesses with an Argus account. Their servers authenticate
  with an API key. **This library is for those servers.**
- **End users** are the customers' own users, whose browsers publish video using
  a short-lived join token. They never hold long-lived credentials.

A typical flow: the customer's server requests a join token, hands it to the
browser, the browser streams to the nearest media server, and the customer's
server fetches frames whenever its agent needs one.

## Installation

```bash
go get github.com/furious-luke/argus-go
```

```go
import argus "github.com/furious-luke/argus-go"
```

The module path is `github.com/furious-luke/argus-go`; the package it exports is
named `argus`, so call sites read `argus.New(...)`, `argus.ParseWebhook(...)`,
and so on.

## Authentication

Two distinct credentials are in play; don't confuse them:

| Credential | Lifetime | Holder | Used for |
| --- | --- | --- | --- |
| **API key** | Long-lived, secret | Customer server (this library) | Minting join tokens; sent as `Authorization: ApiKey <key>` |
| **Read token** | Short-lived JWT | Browser, and the customer server for reads | Scoped to one stream; sent as `Authorization: Bearer <token>` when fetching frames |

The read token is returned in `JoinResponse.Token`. Keep your API key on the
server; never ship it to a browser.

## Usage

### Create a client

```go
c := argus.New("https://argus.example.com", "argus_api_key_...")
```

`New` applies a 30s request timeout. To control the transport, timeouts, or TLS,
pass your own `*http.Client`:

```go
c := argus.NewWithHTTPClient(baseURL, apiKey, &http.Client{Timeout: time.Minute})
```

### Mint a join token

```go
join, err := c.JoinStream(ctx)
if err != nil {
    return err
}
// Forward join.Token and join.GatewayURLs to the end user's browser.
// Keep join.StreamID for later reads.
```

To pin a region or configure a change-trigger webhook, use
`JoinStreamWithOptions`:

```go
threshold := 0.85
join, err := c.JoinStreamWithOptions(ctx, &argus.JoinOptions{
    Region: "eu-west-1", // optional; omit to let Argus choose
    Trigger: &argus.TriggerConfig{
        WebhookURL: "https://my-app.example.com/argus/webhook",
        Threshold:  &threshold, // optional change-detection threshold in (0,1]
        Track:      "camera",   // optional; "camera" or "screen"
    },
})
// When a Trigger is configured, join.WebhookSecret holds the signing secret —
// store it to verify webhook deliveries (see below).
```

Region selection is subject to the account's data-residency policy. A requested
region that violates the policy is rejected, surfaced here as an error.

### Fetch a frame

```go
frame, err := c.FetchFrame(ctx, join.GatewayURLs[0], join.StreamID, join.Token, nil)
if err != nil {
    return err
}
// frame is JPEG bytes by default.
```

Override the track, format, or per-request timeout with `FrameOptions`:

```go
frame, err := c.FetchFrame(ctx, gatewayURL, streamID, readToken, &argus.FrameOptions{
    Track:   "screen", // "camera" (default) or "screen"
    Format:  "png",    // "jpeg" (default) or "png"
    Timeout: 5 * time.Second,
})
```

Argus does not return a retained frame as current after media input has stalled.
If no complete sample has arrived for 15 seconds, the frame gateway returns
`503 Service Unavailable`. `FetchFrame` automatically retries this response for
a short, fixed window so a publisher recovery is usually invisible to callers.
If the stream is still stalled after five attempts, it returns a
`*StaleFrameError`; use `errors.Is(err, argus.ErrStaleFrame)` and inspect
`FrameAge` when you need to distinguish this state from other failures. Raw HTTP
callers can inspect `X-Argus-Frame-Age-Ms` and `Retry-After` on stale responses.

### Receive change-trigger webhooks

When a stream is created with a `Trigger`, Argus watches its video and POSTs an
event to your `WebhookURL` whenever the picture changes. Verify and decode each
delivery with `ParseWebhook`, passing the secret from `JoinResponse.WebhookSecret`:

```go
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(r.Body) // pass the EXACT bytes received
    if err != nil {
        http.Error(w, "read body", http.StatusBadRequest)
        return
    }

    secret := h.secretForStream(/* look up by stream */) // your storage
    ev, err := argus.ParseWebhook(secret, r.Header, body)
    switch {
    case errors.Is(err, argus.ErrInvalidSignature),
        errors.Is(err, argus.ErrMissingSignature),
        errors.Is(err, argus.ErrStaleWebhook):
        http.Error(w, "unauthorized", http.StatusUnauthorized)
        return
    case errors.Is(err, argus.ErrMalformedWebhook):
        http.Error(w, "bad request", http.StatusBadRequest)
        return
    case err != nil:
        http.Error(w, "error", http.StatusInternalServerError)
        return
    }

    // ev.StreamID, ev.Track, ev.SSIMScore, ev.Frame ...
    w.WriteHeader(http.StatusOK)
}
```

If you need the stream ID *before* you can look up its secret, call
`ParseWebhook("", header, body)` first to decode without verifying, then call it
again with the resolved secret to verify for real.

**Signature scheme.** Argus signs `"<X-Argus-Timestamp>.<body>"` with
HMAC-SHA256 and sends the result as `X-Argus-Signature: sha256=<hexdigest>`,
alongside `X-Argus-Timestamp`. `ParseWebhook` recomputes and compares it in
constant time, then rejects deliveries whose timestamp is outside a tolerance
window (5 minutes by default) to prevent replay. Tune it with `WithTolerance`;
pass a non-positive duration to disable the staleness check.

Because verification is over the raw bytes, **do not re-marshal the body before
verifying** — that invalidates the signature.

## Errors

`ParseWebhook` returns sentinel errors you can match with `errors.Is`:

| Error | Meaning | Suggested response |
| --- | --- | --- |
| `ErrMissingSignature` | A secret was supplied but the request had no signature header | `401` |
| `ErrInvalidSignature` | The signature did not match the body | `401` |
| `ErrStaleWebhook` | The signed timestamp is outside the tolerance window | `401` |
| `ErrMalformedWebhook` | The body could not be parsed | `400` |

`JoinStream*` and `FetchFrame` return a wrapped error whose message includes the
HTTP status and response body on a non-success response.

## Files

| File | Contents |
| --- | --- |
| `client.go` | `Client`, `New`, `NewWithHTTPClient`, package docs |
| `stream.go` | `JoinStream` / `JoinStreamWithOptions` and their types |
| `frame.go` | `FetchFrame` and `FrameOptions` |
| `webhook.go` | `ParseWebhook`, `WebhookEvent`, sentinel errors |

## Testing

Tests are behavioural specs (`TestSpec_*`) that exercise the client against
in-process fake Argus endpoints — see `client_spec_test.go` for the contracts and
`client_arrange_test.go` / `client_actor_test.go` for the harness:

```bash
go test ./...
go test -run TestSpec -v ./...
```
