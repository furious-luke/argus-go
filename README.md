# Argus Go Client

A small Go client for the Argus video streaming platform. It runs on a
customer's own backend and wraps the three things a server-side integration
needs:

- **Mint join tokens** for new streams, to hand to an end user's browser.
- **Fetch decoded frames** from a regional frame gateway.
- **Subscribe to change notifications** over a regional gateway WebSocket.

The package uses the Go standard library plus `gorilla/websocket` for notification
subscriptions, without pulling in the rest of Argus.

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
named `argus`, so call sites read `argus.New(...)`, `argus.FetchFrame(...)`, and
`c.Subscribe(...)`.

## Authentication

Three distinct credentials are in play; don't confuse them:

| Credential | Lifetime | Holder | Used for |
| --- | --- | --- | --- |
| **API key** | Long-lived, secret | Customer server (this library) | Minting join tokens; sent as `Authorization: ApiKey <key>` |
| **Join token** | Short-lived JWT | Browser | Publishing one stream through signaling |
| **Read token** | ~1-hour JWT | Browser, then customer server | Fetching frames and subscribing to change notifications for one stream in the selected region |

`JoinResponse.Token` is the publishing join token, not the read token. The
gateway returns the read token to the browser during signaling. Relay that value
and `publisher.selectedGatewayURL` back to the customer server for frame reads
and notification subscriptions. The pair must stay together because the read
token is region-scoped. Keep your API key on the server; never ship it to a
browser.

## Usage

### Create a client

```go
c := argus.New("https://argus.example.com", "argus_api_key_...")
```

`New` applies a 30s request timeout. To control the transport, timeouts, proxy,
or TLS, pass your own `*http.Client`:

```go
c := argus.NewWithHTTPClient(baseURL, apiKey, &http.Client{Timeout: time.Minute})
```

When that client uses a standard `*http.Transport`, its proxy, dial, TLS, and
cookie-jar settings are also applied to `Subscribe`'s WebSocket handshake.

### Mint a join token

```go
join, err := c.JoinStream(ctx)
if err != nil {
    return err
}
// Forward join.Token and join.GatewayURLs to the end user's browser.
// Keep join.StreamID for later reads.
```

To pin a region, use `JoinStreamWithOptions`:

```go
join, err := c.JoinStreamWithOptions(ctx, &argus.JoinOptions{
    Region: "eu-west-1", // optional; omit to let Argus choose
})
```

Change-detection parameters are supplied to `Subscribe`, so they are
per-subscription and can change without recreating the stream.

Region selection is subject to the account's data-residency policy. A requested
region that violates the policy is rejected, surfaced here as an error.

### Fetch a frame

```go
// readToken and selectedGatewayURL were relayed after browser signaling.
frame, err := c.FetchFrame(ctx, selectedGatewayURL, join.StreamID, readToken, nil)
if err != nil {
    return err
}
// frame is JPEG bytes by default.
```

`FetchFrame` accepts the raw `ws://`/`wss://…/signal` URL selected by argus-js
and normalizes it to the gateway's HTTP frame endpoint. An already-normalized
`http://`/`https://` gateway base URL also works.

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

### Subscribe to change notifications

Your server opens one WebSocket per stream to its regional frame gateway. The
connection is the subscription: the current frame arrives immediately, followed
by frames for material scene changes. `Subscribe` blocks for the connection's
lifetime, so run it in a goroutine.

After the initial connection succeeds, an unexpected socket loss is reconnected
automatically on the same customer node with bounded backoff. Context cancellation,
`stream_ended`, `superseded`, protocol errors, and explicit gateway rejections remain
terminal.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go func() {
    err := c.Subscribe(ctx, selectedGatewayURL, streamID, readToken,
        &argus.NotifyOptions{
            Track:          "camera",
            Threshold:      0.85,
            PollIntervalMs: 500,
        },
        argus.NotifyHandlers{
            OnFrame: func(ev argus.NotifyEvent) {
                // ev.StreamID, ev.Track, ev.SSIMScore, ev.FrameFormat, ev.Frame ...
            },
            OnTokenExpiring: func() {
                // Prepare for this subscription to end at token expiry. In-place
                // read-token renewal is planned separately.
            },
            OnEnded: func(reason string) {
                // reason is "stream_ended" or "superseded".
            },
        })
    if err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("notify subscription failed: %v", err)
    }
}()
```

`NotifyOptions` fields are optional; zero values use server defaults.
`NotifyEvent.Frame` contains decoded image bytes. A newer subscription for the
same stream supersedes the older one, which receives `OnEnded("superseded")`.

## Errors

`JoinStream*`, `FetchFrame`, and `Subscribe` return wrapped errors. Frame and
stream creation errors include the HTTP status and response body when applicable.
Cancellation of a live subscription returns the context error.

## Files

| File | Contents |
| --- | --- |
| `client.go` | `Client`, `New`, `NewWithHTTPClient`, package docs |
| `stream.go` | `JoinStream` / `JoinStreamWithOptions` and their types |
| `frame.go` | `FetchFrame` and `FrameOptions` |
| `notify.go` | `Subscribe`, `NotifyEvent`, `NotifyOptions`, `NotifyHandlers` |

## Testing

Tests are behavioural specs (`TestSpec_*`) that exercise the client against
in-process fake Argus endpoints — see `client_spec_test.go` for the contracts and
`client_arrange_test.go` / `client_actor_test.go` for the harness:

```bash
go test ./...
go test -run TestSpec -v ./...
```
