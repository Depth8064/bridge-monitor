//go:build windows

package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows ICMP via iphlpapi so no admin rights / raw sockets are required.
var (
	iphlpapi             = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile   = iphlpapi.NewProc("IcmpCreateFile")
	procIcmpCloseHandle  = iphlpapi.NewProc("IcmpCloseHandle")
	procIcmpSendEcho2    = iphlpapi.NewProc("IcmpSendEcho2")
	procIcmpParseReplies = iphlpapi.NewProc("IcmpParseReplies")
)

var payload = []byte("bridge-monitor-probe-payload-32b")

func (p *icmpProber) Probe(ctx context.Context) Result {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	ip, err := resolveIPv4(rctx, p.host)
	if err != nil {
		return Result{Err: shortErr(err)}
	}

	// Deferred first so the buffer outlives IcmpCloseHandle, which cancels any pending write into it.
	reply := make([]byte, 256+len(payload))
	defer runtime.KeepAlive(reply)

	h, _, callErr := procIcmpCreateFile.Call()
	if windows.Handle(h) == windows.InvalidHandle {
		return Result{Err: "IcmpCreateFile: " + callErr.Error()}
	}
	defer procIcmpCloseHandle.Call(h)

	// Synchronous IcmpSendEcho only returns on scheduler ticks (~15.6ms); an event wakes immediately.
	ev, err := windows.CreateEvent(nil, 0, 0, nil)
	if err != nil {
		return Result{Err: "CreateEvent: " + err.Error()}
	}
	defer windows.CloseHandle(ev)

	timeoutMs := max(uint32(p.timeout.Milliseconds()), 1)
	start := clockNow()
	n, _, callErr := procIcmpSendEcho2.Call(
		h,
		uintptr(ev),
		0, 0,
		uintptr(binary.LittleEndian.Uint32(ip)),
		uintptr(unsafe.Pointer(&payload[0])),
		uintptr(len(payload)),
		0,
		uintptr(unsafe.Pointer(&reply[0])),
		uintptr(len(reply)),
		uintptr(timeoutMs),
	)
	if n == 0 && callErr != windows.ERROR_IO_PENDING {
		return Result{Err: icmpErr(callErr)}
	}
	if n == 0 {
		if w, _ := windows.WaitForSingleObject(ev, timeoutMs+1000); w != windows.WAIT_OBJECT_0 {
			return Result{Err: "timeout"}
		}
	}
	rtt := clockSince(start)
	n, _, callErr = procIcmpParseReplies.Call(uintptr(unsafe.Pointer(&reply[0])), uintptr(len(reply)))
	if n == 0 {
		return Result{Err: icmpErr(callErr)}
	}
	// ICMP_ECHO_REPLY: Address uint32, Status uint32, ...
	if status := binary.LittleEndian.Uint32(reply[4:8]); status != 0 {
		return Result{Err: icmpStatus(status)}
	}
	return Result{OK: true, RTT: rtt}
}

func icmpErr(err error) string {
	if en, ok := err.(syscall.Errno); ok {
		return icmpStatus(uint32(en))
	}
	return err.Error()
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
