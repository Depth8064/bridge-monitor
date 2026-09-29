package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	RoleLocal    = "local"
	RoleRemote   = "remote"
	RoleInternet = "internet"

	TypeICMP = "icmp"
	TypeTCP  = "tcp"
)

// Duration accepts Go duration strings like "1s" or "500ms" in JSON, plus days like "30d".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"1s\" or \"30d\": %w", err)
	}
	if days, ok := strings.CutSuffix(s, "d"); ok {
		f, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return fmt.Errorf("invalid duration %q", s)
		}
		d.Duration = time.Duration(f * float64(24*time.Hour))
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type Target struct {
	Name  string `json:"name"`
	Role  string `json:"role"`
	Type  string `json:"type"`
	Host  string `json:"host"`
	Color string `json:"color,omitempty"`
}

// Storage retention per tier; 0 keeps a tier forever.
type Storage struct {
	Raw    Duration `json:"raw"`
	Minute Duration `json:"minute"`
	Hour   Duration `json:"hour"`
	Day    Duration `json:"day"`
}

type Config struct {
	Listen          string   `json:"listen"`
	Interval        Duration `json:"interval"`
	Timeout         Duration `json:"timeout"`
	OutageThreshold int      `json:"outage_threshold"`
	SpikeMs         float64  `json:"spike_ms"`
	Retention       Duration `json:"retention"`
	DataDir         string   `json:"data_dir"`
	ICMPPrivileged  bool     `json:"icmp_privileged"`
	// InternetSide is which side of the bridge the internet uplink is on, relative to this host.
	InternetSide string   `json:"internet_side"`
	Storage      Storage  `json:"storage"`
	Targets      []Target `json:"targets"`
}

func Default() *Config {
	return &Config{
		Listen:          "127.0.0.1:8080",
		Interval:        Duration{time.Second},
		Timeout:         Duration{2 * time.Second},
		OutageThreshold: 3,
		SpikeMs:         100,
		Retention:       Duration{24 * time.Hour},
		DataDir:         "data",
		InternetSide:    RoleLocal,
		Storage: Storage{
			Raw:    Duration{7 * 24 * time.Hour},
			Minute: Duration{30 * 24 * time.Hour},
			Hour:   Duration{365 * 24 * time.Hour},
		},
	}
}

func (c *Config) InternetIsRemote() bool { return c.InternetSide == RoleRemote }

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := Default()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, c.validate()
}

var colorRe = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

func (c *Config) validate() error {
	var errs []error
	if c.Interval.Duration < 100*time.Millisecond {
		errs = append(errs, errors.New("interval must be >= 100ms"))
	}
	if c.Timeout.Duration <= 0 {
		errs = append(errs, errors.New("timeout must be > 0"))
	}
	if c.OutageThreshold < 1 {
		errs = append(errs, errors.New("outage_threshold must be >= 1"))
	}
	if c.Retention.Duration < time.Minute {
		errs = append(errs, errors.New("retention must be >= 1m"))
	}
	if c.InternetSide != RoleLocal && c.InternetSide != RoleRemote {
		errs = append(errs, errors.New("internet_side must be local or remote"))
	}
	// Memory is refilled from raw samples on startup, so raw must cover it.
	if c.Storage.Raw.Duration < c.Retention.Duration {
		errs = append(errs, errors.New("storage.raw must be >= retention"))
	}
	for name, d := range map[string]time.Duration{"minute": c.Storage.Minute.Duration, "hour": c.Storage.Hour.Duration, "day": c.Storage.Day.Duration} {
		if d < 0 {
			errs = append(errs, fmt.Errorf("storage.%s must be >= 0", name))
		}
	}
	if len(c.Targets) == 0 {
		errs = append(errs, errors.New("at least one target is required"))
	}
	seen := map[string]bool{}
	for i, t := range c.Targets {
		where := fmt.Sprintf("targets[%d] (%q)", i, t.Name)
		if t.Name == "" {
			errs = append(errs, fmt.Errorf("%s: name is required", where))
		} else if seen[t.Name] {
			errs = append(errs, fmt.Errorf("%s: duplicate name", where))
		}
		seen[t.Name] = true
		switch t.Role {
		case RoleLocal, RoleRemote, RoleInternet:
		default:
			errs = append(errs, fmt.Errorf("%s: role must be local, remote or internet", where))
		}
		switch t.Type {
		case TypeICMP:
			if _, _, err := net.SplitHostPort(t.Host); err == nil {
				errs = append(errs, fmt.Errorf("%s: icmp host must not include a port", where))
			}
		case TypeTCP:
			if _, _, err := net.SplitHostPort(t.Host); err != nil {
				errs = append(errs, fmt.Errorf("%s: tcp host must be host:port", where))
			}
		default:
			errs = append(errs, fmt.Errorf("%s: type must be icmp or tcp", where))
		}
		if t.Host == "" {
			errs = append(errs, fmt.Errorf("%s: host is required", where))
		}
		if t.Color != "" && !colorRe.MatchString(t.Color) {
			errs = append(errs, fmt.Errorf("%s: color must look like #rrggbb", where))
		}
	}
	return errors.Join(errs...)
}
