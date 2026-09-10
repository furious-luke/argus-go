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
