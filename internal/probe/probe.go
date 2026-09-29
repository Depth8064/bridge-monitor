package probe

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

type Result struct {
	OK  bool
	RTT time.Duration
	Err string
}

type Prober interface {
	Probe(ctx context.Context) Result
}

func New(t config.Target, timeout time.Duration, privileged bool) Prober {
	if t.Type == config.TypeTCP {
		return &tcpProber{addr: t.Host, timeout: timeout}
	}
	return &icmpProber{host: t.Host, timeout: timeout, privileged: privileged}
}

type tcpProber struct {
	addr    string
	timeout time.Duration
}

func (p *tcpProber) Probe(ctx context.Context) Result {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var d net.Dialer
	start := time.Now()
	c, err := d.DialContext(ctx, "tcp", p.addr)
	rtt := time.Since(start)
	if err != nil {
		return Result{Err: shortErr(err)}
	}
	c.Close()
	return Result{OK: true, RTT: rtt}
}

type icmpProber struct {
	host       string
	timeout    time.Duration
	privileged bool
}

func resolveIPv4(ctx context.Context, host string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4, nil
		}
		return nil, errors.New("only IPv4 is supported")
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil {
		return nil, err
	}
	return ips[0].To4(), nil
}

func shortErr(err error) string {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "timeout"
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "refused"):
		return "connection refused"
	case strings.Contains(s, "unreachable"):
		return "unreachable"
	case strings.Contains(s, "no such host"):
		return "dns: no such host"
	}
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}
