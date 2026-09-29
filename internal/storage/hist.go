package storage

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
)

// Hist is a sparse log-bucketed latency histogram. Buckets grow by histGrowth (~4% relative
// error), so percentiles stay accurate after any number of merges across rollup tiers.
type Hist map[uint16]uint32

const (
	histMinMs  = 0.01
	histGrowth = 1.04
)

var histLog = math.Log(histGrowth)

func histIndex(ms float64) uint16 {
	if ms <= histMinMs {
		return 0
	}
	return uint16(min(math.Floor(math.Log(ms/histMinMs)/histLog)+1, math.MaxUint16))
}

// histValue returns the geometric midpoint of a bucket.
func histValue(i uint16) float64 {
	if i == 0 {
		return histMinMs
	}
	return histMinMs * math.Exp((float64(i)-0.5)*histLog)
}

func (h Hist) Add(ms float64) { h[histIndex(ms)]++ }

func (h Hist) Merge(o Hist) {
	for k, v := range o {
		h[k] += v
	}
}

// Quantile returns the q-th quantile (0..1), or 0 when empty.
func (h Hist) Quantile(q float64) float64 {
	var total uint64
	for _, v := range h {
		total += uint64(v)
	}
	if total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(total)))
	rank = max(rank, 1)
	keys := h.sortedKeys()
	var cum uint64
	for _, k := range keys {
		cum += uint64(h[k])
		if cum >= rank {
			return histValue(k)
		}
	}
	return histValue(keys[len(keys)-1])
}

func (h Hist) sortedKeys() []uint16 {
	keys := make([]uint16, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Encode writes (index delta, count) uvarint pairs in index order.
func (h Hist) Encode() []byte {
	if len(h) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(h)*3)
	var prev uint16
	for _, k := range h.sortedKeys() {
		buf = binary.AppendUvarint(buf, uint64(k-prev))
		buf = binary.AppendUvarint(buf, uint64(h[k]))
		prev = k
	}
	return buf
}

func DecodeHist(b []byte, into Hist) error {
	var idx uint64
	for len(b) > 0 {
		d, n := binary.Uvarint(b)
		if n <= 0 {
			return errors.New("hist: bad index")
		}
		b = b[n:]
		c, n := binary.Uvarint(b)
		if n <= 0 {
			return errors.New("hist: bad count")
		}
		b = b[n:]
		idx += d
		if idx > math.MaxUint16 {
			return errors.New("hist: index out of range")
		}
		into[uint16(idx)] += uint32(c)
	}
	return nil
}
