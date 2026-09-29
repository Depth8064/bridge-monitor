//go:build windows

package probe

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Go's clock on Windows only advances every ~0.5-1ms, which hides sub-ms LAN latency,
// so probes are timed with QueryPerformanceCounter instead.
var (
	kernel32     = windows.NewLazySystemDLL("kernel32.dll")
	procQPC      = kernel32.NewProc("QueryPerformanceCounter")
	procQPF      = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFrequency = func() int64 {
		var f int64
		procQPF.Call(uintptr(unsafe.Pointer(&f)))
		return f
	}()
)

func clockNow() int64 {
	var c int64
	procQPC.Call(uintptr(unsafe.Pointer(&c)))
	return c
}

func clockSince(start int64) time.Duration {
	if qpcFrequency <= 0 {
		return 0
	}
	return time.Duration((clockNow() - start) * int64(time.Second) / qpcFrequency)
}
