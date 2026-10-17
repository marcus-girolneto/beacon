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

const saltSize = 32

const secondsPerDay = 24 * 60 * 60

// VisitorHasher hashes host|ip|ua with an in-memory salt rotated daily.
type VisitorHasher struct {
	random io.Reader

	mu      sync.Mutex
	current atomic.Pointer[daySalt]
}

type daySalt struct {
	day  int64
	salt []byte
}

// NewVisitorHasher returns a hasher reading salts from random (nil means crypto/rand).
func NewVisitorHasher(random io.Reader) *VisitorHasher {
	if random == nil {
		random = rand.Reader
	}
	return &VisitorHasher{random: random}
}

// Hash returns the hex visitor hash, stable within a UTC day.
func (h *VisitorHasher) Hash(now time.Time, host, ip, ua string) (string, error) {
	salt, err := h.saltFor(now)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, salt)
	_, _ = io.WriteString(mac, host)
	_, _ = mac.Write([]byte{0})
	_, _ = io.WriteString(mac, ip)
	_, _ = mac.Write([]byte{0})
	_, _ = io.WriteString(mac, ua)

	var sum [sha256.Size]byte
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
	// don't zero the old salt, concurrent callers may still use it
	h.current.Store(&daySalt{day: day, salt: salt})
	return salt, nil
}
