//go:build linux

package probe

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/Depth8064/bridge-monitor/internal/config"
)

func TestICMPLoopback(t *testing.T) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, unix.IPPROTO_ICMP)
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) {
		t.Skip("unprivileged ICMP not allowed (net.ipv4.ping_group_range)")
	}
	if err == nil {
		unix.Close(fd)
	}

	p := New(config.Target{Type: config.TypeICMP, Host: "127.0.0.1"}, time.Second, false)
	for range 5 {
		r := p.Probe(context.Background())
		if !r.OK {
			t.Fatalf("loopback probe failed: %s", r.Err)
		}
		if r.RTT <= 0 || r.RTT > 50*time.Millisecond {
			t.Fatalf("implausible loopback rtt %v", r.RTT)
		}
		t.Logf("rtt %v", r.RTT)
	}
}

func TestICMPRawLoopback(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("raw ICMP sockets need root or CAP_NET_RAW")
	}
	p := New(config.Target{Type: config.TypeICMP, Host: "127.0.0.1"}, time.Second, true)
	for range 3 {
		if r := p.Probe(context.Background()); !r.OK || r.RTT <= 0 || r.RTT > 50*time.Millisecond {
			t.Fatalf("raw loopback probe: %+v", r)
		}
	}
}

func TestICMPTimeout(t *testing.T) {
	// TEST-NET-1 is reserved and should never answer.
	p := New(config.Target{Type: config.TypeICMP, Host: "192.0.2.1"}, 300*time.Millisecond, false)
	start := time.Now()
	r := p.Probe(context.Background())
	if r.OK {
		t.Skip("192.0.2.1 unexpectedly answered")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("timeout took %v", el)
	}
}

func TestChecksum(t *testing.T) {
	msg := echoRequest(0x1234, 7)
	if checksum(msg) != 0 {
		t.Fatalf("checksum over a message including its checksum should be 0, got %#x", checksum(msg))
	}
}
