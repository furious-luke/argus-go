package argus

// AgentConfig is the optional realtime-agent configuration for a stream, set at
// creation. When present with Mode "realtime", the media server hosts a
// conversational voice agent for the stream (an OpenAI Realtime session) in place
// of the brokered transcript-out / utterance-in loop: the customer server drives
// the agent's behavior at runtime over the notify socket (Configure / UpdatePrompt
// / SendMessage) rather than synthesizing responses itself.
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
	// (Configure / UpdatePrompt carry instructions only).
	Voice string `json:"voice,omitempty"`
}

// AgentModeRealtime is the only agent mode: a realtime conversational agent.
const AgentModeRealtime = "realtime"
