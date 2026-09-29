//go:build !windows && !linux

package probe

import (
	"context"

	probing "github.com/prometheus-community/pro-bing"
)

func (p *icmpProber) Probe(ctx context.Context) Result {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	ip, err := resolveIPv4(rctx, p.host)
	if err != nil {
		return Result{Err: shortErr(err)}
	}

	pinger := probing.New(ip.String())
	pinger.Count = 1
	pinger.Timeout = p.timeout
	pinger.SetPrivileged(p.privileged)
	if err := pinger.Resolve(); err != nil {
		return Result{Err: shortErr(err)}
	}
	if err := pinger.RunWithContext(ctx); err != nil {
		return Result{Err: shortErr(err)}
	}
	st := pinger.Statistics()
	if st.PacketsRecv == 0 || len(st.Rtts) == 0 {
		return Result{Err: "timeout"}
	}
	return Result{OK: true, RTT: st.Rtts[0]}
}
