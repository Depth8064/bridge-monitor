//go:build windows

package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows ICMP via iphlpapi so no admin rights / raw sockets are required.
var (
	iphlpapi            = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile  = iphlpapi.NewProc("IcmpCreateFile")
	procIcmpCloseHandle = iphlpapi.NewProc("IcmpCloseHandle")
	procIcmpSendEcho    = iphlpapi.NewProc("IcmpSendEcho")
)

var payload = []byte("bridge-monitor-probe-payload-32b")

func (p *icmpProber) Probe(ctx context.Context) Result {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	ip, err := resolveIPv4(rctx, p.host)
	if err != nil {
		return Result{Err: shortErr(err)}
	}

	h, _, callErr := procIcmpCreateFile.Call()
	if windows.Handle(h) == windows.InvalidHandle {
		return Result{Err: "IcmpCreateFile: " + callErr.Error()}
	}
	defer procIcmpCloseHandle.Call(h)

	reply := make([]byte, 256+len(payload))
	timeoutMs := max(uint32(p.timeout.Milliseconds()), 1)
	start := time.Now()
	n, _, callErr := procIcmpSendEcho.Call(
		h,
		uintptr(binary.LittleEndian.Uint32(ip)),
		uintptr(unsafe.Pointer(&payload[0])),
		uintptr(len(payload)),
		0,
		uintptr(unsafe.Pointer(&reply[0])),
		uintptr(len(reply)),
		uintptr(timeoutMs),
	)
	rtt := time.Since(start)
	if n == 0 {
		if en, ok := callErr.(syscall.Errno); ok {
			return Result{Err: icmpStatus(uint32(en))}
		}
		return Result{Err: callErr.Error()}
	}
	// ICMP_ECHO_REPLY: Address uint32, Status uint32, ...
	if status := binary.LittleEndian.Uint32(reply[4:8]); status != 0 {
		return Result{Err: icmpStatus(status)}
	}
	return Result{OK: true, RTT: rtt}
}

func icmpStatus(code uint32) string {
	switch code {
	case 11010:
		return "timeout"
	case 11002:
		return "net unreachable"
	case 11003:
		return "host unreachable"
	case 11005:
		return "port unreachable"
	case 11013:
		return "ttl expired"
	case 11050:
		return "general failure"
	}
	return fmt.Sprintf("icmp status %d", code)
}
