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

	"github.com/Depth8064/bridge-monitor/internal/model"
	"github.com/Depth8064/bridge-monitor/internal/monitor"
	"github.com/Depth8064/bridge-monitor/internal/storage"
)

//go:embed static
var static embed.FS

func Handler(eng *monitor.Engine, hist *storage.History, db *storage.DB) http.Handler {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(sub))
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		win := window(r)
		if !hist.Handles(win) {
			writeJSON(w, eng.State(win))
			return
		}
		st, err := hist.State(r.Context(), win)
		if err != nil {
			serverError(w, "history state", err)
			return
		}
		writeJSON(w, st)
	})
	mux.HandleFunc("GET /api/series", func(w http.ResponseWriter, r *http.Request) {
		win, buckets := window(r), intParam(r, "buckets", 300, 10, 2000)
		if !hist.Handles(win) {
			writeJSON(w, eng.Series(win, buckets))
			return
		}
		se, err := hist.Series(r.Context(), win, buckets)
		if err != nil {
			serverError(w, "history series", err)
			return
		}
		writeJSON(w, se)
	})
	mux.HandleFunc("GET /api/outages.csv", func(w http.ResponseWriter, r *http.Request) {
		win := window(r)
		outs := eng.Outages(win)
		if hist.Handles(win) {
			var err error
			if outs, err = hist.Outages(r.Context(), win); err != nil {
				serverError(w, "history outages", err)
				return
			}
		}
		writeOutagesCSV(w, outs)
	})
	mux.HandleFunc("GET /api/samples.csv", func(w http.ResponseWriter, r *http.Request) {
		var from time.Time
		if win := window(r); win > 0 {
			from = time.Now().Add(-win)
		}
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="bridge-samples.csv"`)
		if err := db.ExportSamples(r.Context(), w, from); err != nil {
			log.Printf("export samples: %v", err)
		}
	})
	return securityHeaders(mux)
}

func writeOutagesCSV(w http.ResponseWriter, outs []model.Outage) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="bridge-outages.csv"`)
	cw := csv.NewWriter(w)
	cw.Write([]string{"target", "role", "start", "end", "duration_s", "lost_probes", "ongoing"})
	for _, o := range outs {
		cw.Write([]string{
			o.Target, o.Role, o.Start.Format(time.RFC3339), o.End.Format(time.RFC3339),
			strconv.FormatFloat(o.Duration, 'f', 3, 64), strconv.Itoa(o.Lost), strconv.FormatBool(o.Ongoing),
		})
	}
	cw.Flush()
}

func serverError(w http.ResponseWriter, what string, err error) {
	log.Printf("%s: %v", what, err)
	http.Error(w, "internal error", http.StatusInternalServerError)
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
	return time.Duration(min(max(n, 10), 10*365*86400)) * time.Second
}

func intParam(r *http.Request, name string, def, lo, hi int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil {
		return def
	}
	return min(max(n, lo), hi)
}
