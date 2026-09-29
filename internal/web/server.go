package web

import (
	"embed"
	"encoding/csv"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/monitor"
)

//go:embed static
var static embed.FS

func Handler(eng *monitor.Engine) http.Handler {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(sub))
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, eng.State(window(r)))
	})
	mux.HandleFunc("GET /api/series", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, eng.Series(window(r), intParam(r, "buckets", 300, 10, 2000)))
	})
	mux.HandleFunc("GET /api/outages.csv", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="bridge-outages.csv"`)
		cw := csv.NewWriter(w)
		cw.Write([]string{"target", "role", "start", "end", "duration_s", "lost_probes", "ongoing"})
		for _, o := range eng.Outages(window(r)) {
			cw.Write([]string{
				o.Target, o.Role, o.Start.Format(time.RFC3339), o.End.Format(time.RFC3339),
				strconv.FormatFloat(o.Duration, 'f', 3, 64), strconv.Itoa(o.Lost), strconv.FormatBool(o.Ongoing),
			})
		}
		cw.Flush()
	})
	return securityHeaders(mux)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

// window reads ?window=<seconds>; 0 means all retained data.
func window(r *http.Request) time.Duration {
	s := r.URL.Query().Get("window")
	if s == "" {
		return 15 * time.Minute
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 15 * time.Minute
	}
	if n <= 0 {
		return 0
	}
	return time.Duration(min(max(n, 10), 90*86400)) * time.Second
}

func intParam(r *http.Request, name string, def, lo, hi int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil {
		return def
	}
	return min(max(n, lo), hi)
}
