//go:build linux

package probe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Replies are timed with the kernel's receive timestamp (SO_TIMESTAMPNS), so Go scheduling and
// vCPU wake-up after the packet arrived are not counted as network latency.

var (
	icmpSeq atomic.Uint32
	icmpID  = uint16(rand.Uint32())
	payload = []byte("bridge-monitor-probe-payload-32b")
)

func (p *icmpProber) Probe(ctx context.Context) Result {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	ip, err := resolveIPv4(rctx, p.host)
	if err != nil {
		return Result{Err: shortErr(err)}
	}

	// Unprivileged "ping" sockets need net.ipv4.ping_group_range; raw sockets need CAP_NET_RAW.
	typ := unix.SOCK_DGRAM
	if p.privileged {
		typ = unix.SOCK_RAW
	}
	fd, err := unix.Socket(unix.AF_INET, typ|unix.SOCK_CLOEXEC, unix.IPPROTO_ICMP)
	if err != nil {
		return Result{Err: "icmp socket: " + err.Error()}
	}
	defer unix.Close(fd)
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1); err != nil {
		return Result{Err: "SO_TIMESTAMPNS: " + err.Error()}
	}

	seq := uint16(icmpSeq.Add(1))
	dst := &unix.SockaddrInet4{Addr: [4]byte(ip.To4())}
	msg := echoRequest(icmpID, seq)
	deadline := time.Now().Add(p.timeout)

	sent := time.Now()
	if err := unix.Sendto(fd, msg, 0, dst); err != nil {
		return Result{Err: shortErr(err)}
	}

	buf := make([]byte, 1500)
	oob := make([]byte, 128)
	for {
		wait := time.Until(deadline)
		if wait <= 0 {
			return Result{Err: "timeout"}
		}
		n, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, int(wait.Milliseconds())+1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return Result{Err: "poll: " + err.Error()}
		}
		if n == 0 {
			return Result{Err: "timeout"}
		}
		n, oobn, _, _, err := unix.Recvmsg(fd, buf, oob, unix.MSG_DONTWAIT)
		userTime := time.Now()
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return Result{Err: shortErr(err)}
		}
		data := buf[:n]
		if p.privileged {
			// Raw sockets see every ICMP packet with its IP header; ICMP errors come from routers, replies from the target.
			if len(data) < 20 || len(data) < int(data[0]&0x0f)*4 {
				continue
			}
			src := [4]byte(data[12:16])
			data = data[int(data[0]&0x0f)*4:]
			if len(data) > 0 && data[0] == 0 && src != dst.Addr {
				continue
			}
		}
		ok, errText := matchReply(data, seq, p.privileged)
		if !ok && errText == "" {
			continue
		}
		if errText != "" {
			return Result{Err: errText}
		}
		rtt := userTime.Sub(sent)
		if kt, found := kernelTimestamp(oob[:oobn]); found {
			// Kernel and time.Now both use CLOCK_REALTIME; fall back if a clock step makes it implausible.
			if k := kt.Sub(sent); k > 0 && k <= rtt {
				rtt = k
			}
		}
		return Result{OK: true, RTT: rtt}
	}
}

func echoRequest(id, seq uint16) []byte {
	msg := make([]byte, 8+len(payload))
	msg[0] = 8 // echo request
	binary.BigEndian.PutUint16(msg[4:], id)
	binary.BigEndian.PutUint16(msg[6:], seq)
	copy(msg[8:], payload)
	binary.BigEndian.PutUint16(msg[2:], checksum(msg))
	return msg
}

func checksum(b []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(b); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(b[i:]))
	}
	if len(b)%2 == 1 {
		sum += uint32(b[len(b)-1]) << 8
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}

// matchReply reports whether data is the echo reply for seq, or an ICMP error about our request.
// Ping sockets rewrite the ICMP id and only deliver our own replies, so id is checked on raw sockets only.
func matchReply(data []byte, seq uint16, raw bool) (bool, string) {
	if len(data) < 8 {
		return false, ""
	}
	switch data[0] {
	case 0: // echo reply
		if binary.BigEndian.Uint16(data[6:]) != seq {
			return false, ""
		}
		if raw && binary.BigEndian.Uint16(data[4:]) != icmpID {
			return false, ""
		}
		return true, ""
	case 3, 11: // destination unreachable, time exceeded: original IP header + 8 bytes follow
		orig := data[8:]
		if len(orig) < 20 {
			return false, ""
		}
		orig = orig[int(orig[0]&0x0f)*4:]
		if len(orig) < 8 || orig[0] != 8 || binary.BigEndian.Uint16(orig[6:]) != seq {
			return false, ""
		}
		if raw && binary.BigEndian.Uint16(orig[4:]) != icmpID {
			return false, ""
		}
		if data[0] == 11 {
			return false, "ttl expired"
		}
		switch data[1] {
		case 0:
			return false, "net unreachable"
		case 1:
			return false, "host unreachable"
		default:
			return false, fmt.Sprintf("unreachable (code %d)", data[1])
		}
	}
	return false, ""
}

func kernelTimestamp(oob []byte) (time.Time, bool) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return time.Time{}, false
	}
	for _, m := range msgs {
		if m.Header.Level == unix.SOL_SOCKET && m.Header.Type == unix.SCM_TIMESTAMPNS &&
			len(m.Data) >= int(unsafe.Sizeof(unix.Timespec{})) {
			ts := (*unix.Timespec)(unsafe.Pointer(&m.Data[0]))
			return time.Unix(ts.Unix()), true
		}
	}
	return time.Time{}, false
}
