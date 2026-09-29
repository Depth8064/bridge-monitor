package model

import (
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

// Bridge outages are derived from rounds rather than probed, and are stored under this pseudo-target.
const (
	BridgeTarget = "Bridge"
	RoleBridge   = "bridge"
)

type Sample struct {
	Target string
	OK     bool
	RTT    time.Duration
	Err    string
}

type Outage struct {
	Target   string    `json:"target"`
	Role     string    `json:"role"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	Duration float64   `json:"duration_s"`
	Lost     int       `json:"lost"`
	Ongoing  bool      `json:"ongoing"`
}

func NewOutage(target, role string, start, end time.Time, lost int, ongoing bool) Outage {
	return Outage{
		Target: target, Role: role, Start: start, End: end,
		Duration: end.Sub(start).Seconds(), Lost: lost, Ongoing: ongoing,
	}
}

// Round holds per-role health for one probe round: -1 no targets, 0 any failed, 1 all ok.
type Round struct {
	Local, Remote, Inet int8
}

func NewRound() Round { return Round{Local: -1, Remote: -1, Inet: -1} }

func (r *Round) Apply(role string, ok bool) {
	var p *int8
	switch role {
	case config.RoleLocal:
		p = &r.Local
	case config.RoleRemote:
		p = &r.Remote
	case config.RoleInternet:
		p = &r.Inet
	default:
		return
	}
	if !ok {
		*p = 0
	} else if *p == -1 {
		*p = 1
	}
}

// FarSide is the health of the far side of the bridge: remote targets, or internet
// targets when the uplink is across the bridge and no remote targets exist.
func (r Round) FarSide(inetRemote bool) int8 {
	if inetRemote && r.Remote == -1 {
		return r.Inet
	}
	return r.Remote
}

// BridgeFault: the far side failed while everything on this side of the bridge was healthy.
// A local uplink is a control; a remote uplink sits behind the bridge so it can't be one.
func (r Round) BridgeFault(inetRemote bool) bool {
	if r.FarSide(inetRemote) != 0 || r.Local == 0 {
		return false
	}
	return inetRemote || r.Inet != 0
}

// InetBridge: internet was down because the bridge was.
func (r Round) InetBridge(inetRemote bool) bool {
	return r.Inet == 0 && r.Local != 0 && r.BridgeFault(inetRemote)
}

// Upstream: internet was down but not because of the bridge. Behind the bridge this is only
// claimed when the remote site is provably reachable.
func (r Round) Upstream(inetRemote bool) bool {
	return r.Inet == 0 && r.Local != 0 && !r.BridgeFault(inetRemote) && (!inetRemote || r.Remote == 1)
}
