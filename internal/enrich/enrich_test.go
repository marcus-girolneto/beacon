package enrich

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// counterReader yields a distinct, deterministic salt on every read so tests
// can observe rotation.
type counterReader struct{ n byte }

func (r *counterReader) Read(p []byte) (int, error) {
	r.n++
	for i := range p {
		p[i] = r.n
	}
	return len(p), nil
}

var day1 = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

func TestVisitorHasherStableWithinDay(t *testing.T) {
	t.Parallel()
	h := NewVisitorHasher(&counterReader{})

	a, err := h.Hash(day1, "example.com", "203.0.113.7", "ua")
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.Hash(day1.Add(14*time.Hour+59*time.Minute), "example.com", "203.0.113.7", "ua")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("hash changed within the same UTC day: %s != %s", a, b)
	}
	if len(a) != 32 {
		t.Errorf("hash length = %d, want 32 hex chars", len(a))
	}
	if strings.Contains(a, "203.0.113.7") {
		t.Error("hash leaks the IP address")
	}
}

func TestVisitorHasherRotatesDaily(t *testing.T) {
	t.Parallel()
	h := NewVisitorHasher(&counterReader{})

	today, err := h.Hash(day1, "example.com", "203.0.113.7", "ua")
	if err != nil {
		t.Fatal(err)
	}
	tomorrow, err := h.Hash(day1.Add(24*time.Hour), "example.com", "203.0.113.7", "ua")
	if err != nil {
		t.Fatal(err)
	}
	if today == tomorrow {
		t.Error("hash did not change across UTC days")
	}
}

func TestVisitorHasherSeparatesInputs(t *testing.T) {
	t.Parallel()
	h := NewVisitorHasher(nil)
	base, _ := h.Hash(day1, "example.com", "203.0.113.7", "ua")
	for name, args := range map[string][3]string{
		"other host": {"other.com", "203.0.113.7", "ua"},
		"other ip":   {"example.com", "203.0.113.8", "ua"},
		"other ua":   {"example.com", "203.0.113.7", "ua2"},
		"shifted":    {"example.com2", "03.0.113.7", "ua"},
	} {
		got, _ := h.Hash(day1, args[0], args[1], args[2])
		if got == base {
			t.Errorf("%s: hash collides with base", name)
		}
	}
}

func TestVisitorHasherRandomError(t *testing.T) {
	t.Parallel()
	boom := errors.New("entropy exhausted")
	h := NewVisitorHasher(iotest.ErrReader(boom))
	if _, err := h.Hash(day1, "h", "ip", "ua"); !errors.Is(err, boom) {
		t.Fatalf("Hash() error = %v, want %v", err, boom)
	}
}

func TestVisitorHasherConcurrentRotation(t *testing.T) {
	t.Parallel()
	h := NewVisitorHasher(nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 200 {
				ts := day1.Add(time.Duration(i+j) * 6 * time.Hour)
				if _, err := h.Hash(ts, "h", "ip", "ua"); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestEnrich(t *testing.T) {
	t.Parallel()
	en := New(NewVisitorHasher(bytes.NewReader(bytes.Repeat([]byte{7}, 64))))
	received := day1.Add(time.Hour)
	meta := RequestMeta{
		IP:         "198.51.100.23",
		UserAgent:  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) Mobile/15E148",
		ReceivedAt: received,
	}

	e := &event.Event{
		MessageID:   "msg-1",
		Type:        event.TypePageview,
		AnonymousID: "anon-1",
		URL:         "https://Shop.Example.com/pricing?utm_source=Google&utm_medium=cpc&utm_campaign=spring&email=a@b.c#top",
		Referrer:    "https://www.google.com/search?q=private",
		UTM:         event.UTM{Campaign: "override"},
		Properties:  json.RawMessage(`{"plan":"pro"}`),
		Timestamp:   day1,
	}
	rec, err := en.Enrich(e, meta)
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		field     string
		got, want any
	}{
		{"MessageID", rec.MessageID, "msg-1"},
		{"URL", rec.URL, "https://Shop.Example.com/pricing"},
		{"Host", rec.Host, "shop.example.com"},
		{"Path", rec.Path, "/pricing"},
		{"Referrer", rec.Referrer, "https://www.google.com/search"},
		{"ReferrerHost", rec.ReferrerHost, "www.google.com"},
		{"Channel", rec.Channel, event.ChannelPaid},
		{"UTM", rec.UTM, event.UTM{Source: "google", Medium: "cpc", Campaign: "override"}},
		{"Device", rec.Device, event.DeviceMobile},
		{"Timestamp", rec.Timestamp, day1},
		{"ReceivedAt", rec.ReceivedAt, received},
		{"Properties", string(rec.Properties), `{"plan":"pro"}`},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.got, c.want)
		}
	}
	if rec.VisitorHash == "" {
		t.Error("VisitorHash is empty")
	}

	encoded, _ := json.Marshal(rec)
	for _, secret := range []string{meta.IP, "iPhone OS", "a@b.c", "q=private"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Errorf("record leaks %q: %s", secret, encoded)
		}
	}
}

func TestEnrichDefaults(t *testing.T) {
	t.Parallel()
	en := New(NewVisitorHasher(nil))
	received := day1
	rec, err := en.Enrich(&event.Event{
		Type: event.TypeClick, AnonymousID: "a", URL: "https://example.com",
	}, RequestMeta{ReceivedAt: received})
	if err != nil {
		t.Fatal(err)
	}
	if rec.MessageID == "" {
		t.Error("MessageID not generated")
	}
	if rec.Path != "/" {
		t.Errorf("Path = %q, want /", rec.Path)
	}
	if !rec.Timestamp.Equal(received) {
		t.Errorf("Timestamp = %v, want receive time %v", rec.Timestamp, received)
	}
	if rec.Channel != event.ChannelDirect || rec.Device != event.DeviceUnknown {
		t.Errorf("Channel/Device = %q/%q, want direct/unknown", rec.Channel, rec.Device)
	}
}

func TestEnrichErrors(t *testing.T) {
	t.Parallel()

	en := New(NewVisitorHasher(nil))
	if _, err := en.Enrich(&event.Event{URL: "https://exa mple.com/%zz"}, RequestMeta{}); err == nil {
		t.Error("Enrich() with unparseable URL succeeded")
	}

	failing := New(NewVisitorHasher(iotest.ErrReader(io.ErrUnexpectedEOF)))
	_, err := failing.Enrich(&event.Event{URL: "https://example.com"}, RequestMeta{ReceivedAt: day1})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("Enrich() error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func BenchmarkEnrich(b *testing.B) {
	en := New(NewVisitorHasher(nil))
	e := &event.Event{
		Type:        event.TypePageview,
		AnonymousID: "anon-1",
		URL:         "https://example.com/pricing?utm_source=google&utm_medium=cpc&utm_campaign=spring",
		Referrer:    "https://www.google.com/",
	}
	meta := RequestMeta{IP: "198.51.100.23", UserAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6)", ReceivedAt: day1}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := en.Enrich(e, meta); err != nil {
			b.Fatal(err)
		}
	}
}
