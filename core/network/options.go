package network

import (
	"time"
)

type Option func(m *NetworkManager)

// WithHeartbeatTimeout configures the transport to not wait for than d for
// heartbeat to be executes by remote peer.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(m *NetworkManager) {
		m.HeartbeatTimeout = d
	}
}
