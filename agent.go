package argus

import (
	"encoding/json"
	"time"
)

// AgentConfig is the optional realtime-agent configuration for a stream, set at
// creation. When present with Mode "realtime", the media server hosts a
// conversational voice agent for the stream (an OpenAI Realtime session) in place
// of the brokered transcript-out / utterance-in loop: the customer server drives
// the agent's behavior at runtime over the notify socket (ConfigureAgent /
// UpdatePrompt / SendMessage) rather than synthesizing responses itself.
//
// It mirrors the control plane's agent config JSON shape. Empty fields take fleet
// defaults; a nil *AgentConfig means an ordinary brokered stream.
type AgentConfig struct {
	// Mode selects the agent mode; use AgentModeRealtime.
	Mode string `json:"mode,omitempty"`
	// Provider is the realtime model provider (fleet default when empty).
	Provider string `json:"provider,omitempty"`
	// Voice is the agent voice (fleet default when empty). It is pinned for the
	// life of the session — the provider locks voice once the model has produced
	// audio — so it is chosen only here at creation and cannot be changed at runtime
	// (ConfigureAgent / UpdatePrompt cannot change it).
	Voice string `json:"voice,omitempty"`
}

// AgentModeRealtime is the only agent mode: a realtime conversational agent.
const AgentModeRealtime = "realtime"

// AgentTool declares one function the realtime agent may call during a
// conversation. When the agent decides to call it, the customer server receives an
// AgentToolCall (NotifyHandlers.OnAgentToolCall), runs the function, and returns the
// result with NotifySubscription.SubmitToolResult. Argus never executes the tool
// itself — it relays the call and the result.
type AgentTool struct {
	// Name is the function name the model calls.
	Name string
	// Description tells the model when to use the tool.
	Description string
	// Parameters is the JSON Schema (as raw JSON) for the call arguments.
	Parameters json.RawMessage
}

// AgentConfiguration is the full realtime-agent configuration applied by
// NotifySubscription.ConfigureAgent. Each call is a full replacement of the callable
// surface: Instructions sets the system prompt, Tools declares the functions the
// agent may call (an empty slice clears any previously declared tools), ToolChoice
// governs whether the model may/must call them, and ToolTimeout bounds how long a
// single tool call waits for its result before Argus fails it and lets the turn
// resume.
type AgentConfiguration struct {
	// Instructions is the agent's system prompt.
	Instructions string
	// Tools are the functions the agent may call. Empty clears the tool set.
	Tools []AgentTool
	// ToolChoice is "auto" (default when empty), "none", or "required".
	ToolChoice string
	// ToolTimeout is the per-call deadline; zero uses the server default (10s), and
	// the server caps larger values at one minute.
	ToolTimeout time.Duration
}

// AgentToolCall is a function-call request from the realtime agent, delivered to
// NotifyHandlers.OnAgentToolCall. The customer server runs the named function with
// the given raw-JSON arguments and returns the result with
// NotifySubscription.SubmitToolResult (or SubmitToolError), correlating by CallID.
type AgentToolCall struct {
	CallID    string
	Name      string
	Arguments string
}

// AgentToolFailure reports that Argus can no longer accept a result for a tool
// call, for example because its deadline expired or its provider session was lost.
type AgentToolFailure struct {
	CallID string
	Reason string
}

// Stable reasons carried by AgentToolFailure.Reason.
const (
	AgentToolFailureReasonTimeout     = "tool call timed out"
	AgentToolFailureReasonSessionLost = "provider session lost during tool call"
)

// WorkerReadiness is a worker strand's content state.
type WorkerReadiness string

const (
	WorkerWorking WorkerReadiness = "working" // producing; nothing to deliver yet
	WorkerReady   WorkerReadiness = "ready"   // content waiting to be delivered
	WorkerDone    WorkerReadiness = "done"    // finished; the strand is retired
)

// WorkerFloor is how a strand competes for the conversational floor.
type WorkerFloor string

const (
	FloorAdopts   WorkerFloor = "adopts"   // take the floor only when nothing else holds it
	FloorReclaims WorkerFloor = "reclaims" // hold/retake the floor for the strand's active life
	FloorSeizes   WorkerFloor = "seizes"   // interrupt to take the floor while it has content
)

// WorkerMultiplicity is whether a kind keeps one strand or many.
type WorkerMultiplicity string

const (
	Singleton  WorkerMultiplicity = "singleton"  // a new unit supersedes the prior
	Concurrent WorkerMultiplicity = "concurrent" // many coexist up to ConcurrentCap
)

// NoticeEnvelope frames a backgrounded update that does not hold the floor.
type NoticeEnvelope string

const (
	Informational NoticeEnvelope = "informational" // mention it; don't act unless asked
	Actionable    NoticeEnvelope = "actionable"    // act on this as instructed
)

// WorkerTraits are the attention traits a worker declares for a strand so Argus can
// arbitrate it without knowing the worker's type. They are honored when the strand is
// first created (its first push); empty fields take Argus's defaults (adopts,
// singleton, informational).
type WorkerTraits struct {
	Multiplicity   WorkerMultiplicity
	Floor          WorkerFloor
	Priority       int
	NoticeEnvelope NoticeEnvelope
	// ConcurrentCap bounds live strands of a Concurrent kind; zero uses the default.
	ConcurrentCap int
}

// WorkerPushRejection reports that a PushWorker update was not accepted by the agent, so
// its content was not delivered. The worker should hold the unit and retry — a successful
// PushWorker call only means the update reached the server, not that it was accepted.
// WorkerKind and StrandID identify the rejected push; Reason carries why.
type WorkerPushRejection struct {
	WorkerKind string
	StrandID   string
	Reason     string
}

// Stable reasons carried by WorkerPushRejection.Reason.
const (
	// WorkerPushRejectionReasonConcurrentCap means a concurrent kind was at its
	// per-stream cap with every live strand still working, so the new unit was dropped
	// rather than evicting in-flight work.
	WorkerPushRejectionReasonConcurrentCap = "concurrent strand cap reached"
	// WorkerPushRejectionReasonInvalidIdentity means the update was missing its worker
	// kind or strand id; both are required to identify a strand.
	WorkerPushRejectionReasonInvalidIdentity = "worker kind and strand id are required"
)

// WorkerUpdate is one push from a background worker on the customer server to the
// agent, sent with NotifySubscription.PushWorker. WorkerKind identifies the producing
// worker and StrandID the stream of output it maintains; Content is the already-
// prepared text to voice or consider (present when Readiness is WorkerReady). Awaited
// marks the deferred result of a "being prepared" tool call the user is waiting for,
// so Argus delivers it directly rather than as a passing notice. Traits are honored on
// the strand's first push.
type WorkerUpdate struct {
	WorkerKind string
	StrandID   string
	Readiness  WorkerReadiness
	Content    string
	Awaited    bool
	Traits     *WorkerTraits
}
