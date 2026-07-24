package argus

// TrackType identifies the logical purpose of a video track within a stream. A
// stream may carry more than one track (e.g. a camera and a screen share); reads
// and change-notification subscriptions address a specific track by type.
type TrackType = string

const (
	// TrackCamera is the default track type, a camera capture.
	TrackCamera TrackType = "camera"
	// TrackScreen is a screen / window / tab share.
	TrackScreen TrackType = "screen"
)
