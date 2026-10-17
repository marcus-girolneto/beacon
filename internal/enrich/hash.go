package enrich

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// saltSize is the size in bytes of each daily salt.
const saltSize = 32

const secondsPerDay = 24 * 60 * 60

// VisitorHasher derives a pseudonymous visitor identifier from the client IP
// address and User-Agent without ever storing either.
//
// The identifier is HMAC-SHA256(salt_day, host | ip | ua), where salt_day is
// a random 256-bit key generated per UTC day and kept only in memory. Once
// the day rolls over the previous salt is discarded, so hashes from different
// days cannot be linked and nobody, including the operator, can recompute a
// hash from a known IP after the fact. Including the site host prevents the
// same visitor from being correlated across sites served by one collector.
type VisitorHasher struct {
	random io.Reader

	mu      sync.Mutex // serializes salt rotation
	current atomic.Pointer[daySalt]
}

type daySalt struct {
	day  int64
	salt []byte
}

// NewVisitorHasher returns a hasher that draws salts from random. A nil
// random uses crypto/rand.
func NewVisitorHasher(random io.Reader) *VisitorHasher {
	if random == nil {
		random = rand.Reader
	}
	return &VisitorHasher{random: random}
}

// Hash returns the hex-encoded visitor hash for the given request attributes
// at time now. The result is stable within a UTC day and unrelated across
// days.
func (h *VisitorHasher) Hash(now time.Time, host, ip, ua string) (string, error) {
	salt, err := h.saltFor(now)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, salt)
	// Writes to a hash.Hash never return an error.
	_, _ = io.WriteString(mac, host)
	_, _ = mac.Write([]byte{0})
	_, _ = io.WriteString(mac, ip)
	_, _ = mac.Write([]byte{0})
	_, _ = io.WriteString(mac, ua)

	var sum [sha256.Size]byte
	// 128 bits is plenty to avoid collisions among one day's visitors while
	// keeping rows compact.
	return hex.EncodeToString(mac.Sum(sum[:0])[:16]), nil
}

func (h *VisitorHasher) saltFor(now time.Time) ([]byte, error) {
	day := now.Unix() / secondsPerDay
	if cur := h.current.Load(); cur != nil && cur.day == day {
		return cur.salt, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if cur := h.current.Load(); cur != nil && cur.day == day {
		return cur.salt, nil
	}
	salt := make([]byte, saltSize)
	if _, err := io.ReadFull(h.random, salt); err != nil {
		return nil, fmt.Errorf("generate daily salt: %w", err)
	}
	// The previous salt is dropped rather than zeroed: concurrent callers may
	// still be hashing with it, and wiping it would corrupt their results.
	h.current.Store(&daySalt{day: day, salt: salt})
	return salt, nil
}
