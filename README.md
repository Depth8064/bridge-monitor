# bridge-monitor

Continuously probes a point-to-point wireless bridge (local site, remote site) plus internet
control targets, logs every probe to CSV, and serves a live dashboard with loss, latency,
outage and "bridge-attributable" statistics.

## Run

```sh
cp config.example.json config.json   # edit targets
go build -o bridge-monitor .
./bridge-monitor -config config.json
```

Open http://127.0.0.1:8080. Set `"listen": ":8080"` to view it from other machines.

- **Windows:** ICMP uses `IcmpSendEcho`, so admin rights aren't needed.
- **Linux:** uses unprivileged ICMP by default (`net.ipv4.ping_group_range`). If that is
  restricted, set `"icmp_privileged": true` and run as root or grant `CAP_NET_RAW`.

## Config

| Field | Meaning |
|---|---|
| `interval` / `timeout` | Probe cadence and per-probe timeout (rounds may overlap) |
| `outage_threshold` | Consecutive lost probes that count as an outage |
| `spike_ms` | Replies slower than this are counted as latency spikes |
| `retention` | How much history is kept in memory / reloaded on restart |
| `targets[].role` | `local`, `remote` (across the bridge) or `internet` (control) |
| `targets[].type` | `icmp` (`host`) or `tcp` (`host:port`) |

## Bridge verdict

Every round probes all targets at the same time. A round is a **bridge fault** when a
`remote` target fails while every `local` and `internet` target succeeds. This means the
monitoring host, the LAN and the uplink were all working when the remote site dropped.

## Data

- `data/samples-YYYY-MM-DD.csv`: every probe (time, target, ok, rtt, error)
- `data/outages.csv`: every completed outage
- Dashboard **Download CSV**: outages for the selected window
