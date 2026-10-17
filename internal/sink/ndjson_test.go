package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/event"
)

func sampleRecords(n int) []event.Record {
	recs := make([]event.Record, n)
	for i := range recs {
		recs[i] = event.Record{
			MessageID:  string(rune('a' + i%26)),
			Type:       event.TypePageview,
			Channel:    event.ChannelSearch,
			Device:     event.DeviceDesktop,
			Properties: json.RawMessage(`{"i":1}`),
			Timestamp:  time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		}
	}
	return recs
}

func TestNDJSONWritesOneLinePerRecord(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	s := NewNDJSON(&buf)
	if err := s.Write(t.Context(), sampleRecords(3)); err != nil {
		t.Fatal(err)
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3:\n%s", len(lines), buf.String())
	}
	var rec event.Record
	if err := json.Unmarshal(lines[1], &rec); err != nil {
		t.Fatal(err)
	}
	if rec.MessageID != "b" || rec.Channel != event.ChannelSearch {
		t.Errorf("decoded %+v", rec)
	}
}

func TestNDJSONConcurrentWritesDoNotInterleave(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	s := NewNDJSON(&buf)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := s.Write(context.Background(), sampleRecords(200)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	sc := bufio.NewScanner(&buf)
	n := 0
	for sc.Scan() {
		if !json.Valid(sc.Bytes()) {
			t.Fatalf("line %d is not valid JSON: %q", n, sc.Text())
		}
		n++
	}
	if n != 1600 {
		t.Fatalf("got %d lines, want 1600", n)
	}
}

func TestNDJSONHonorsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var buf bytes.Buffer
	if err := NewNDJSON(&buf).Write(ctx, sampleRecords(1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write() = %v, want context.Canceled", err)
	}
	if buf.Len() != 0 {
		t.Error("wrote data despite canceled context")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestNDJSONWriterError(t *testing.T) {
	t.Parallel()
	if err := NewNDJSON(failingWriter{}).Write(t.Context(), sampleRecords(1)); err == nil {
		t.Fatal("Write() succeeded on failing writer")
	}
}

func TestNDJSONEncodeError(t *testing.T) {
	t.Parallel()
	recs := sampleRecords(1)
	recs[0].Properties = json.RawMessage(`{broken`)
	if err := NewNDJSON(&bytes.Buffer{}).Write(t.Context(), recs); err == nil {
		t.Fatal("Write() succeeded with invalid properties")
	}
}

func TestFileAppendsAcrossOpens(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events.ndjson")

	for range 2 {
		f, err := OpenFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Write(t.Context(), sampleRecords(2)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(data, []byte("\n")); got != 4 {
		t.Fatalf("file has %d lines, want 4", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file mode = %o, want 600", perm)
	}
}

func TestFileErrors(t *testing.T) {
	t.Parallel()
	if _, err := OpenFile(filepath.Join(t.TempDir(), "missing", "events.ndjson")); err == nil {
		t.Error("OpenFile() in missing directory succeeded")
	}

	f, err := OpenFile(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Write(t.Context(), sampleRecords(1)); err == nil {
		t.Error("Write() after Close succeeded")
	}
}

// flakyWriter fails its first Write and succeeds afterwards.
type flakyWriter struct {
	failed bool
	bytes.Buffer
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	if !w.failed {
		w.failed = true
		return 0, errors.New("transient")
	}
	return w.Buffer.Write(p)
}

func TestNDJSONRecoversAfterTransientError(t *testing.T) {
	t.Parallel()
	w := &flakyWriter{}
	s := NewNDJSON(w)
	if err := s.Write(t.Context(), sampleRecords(1)); err == nil {
		t.Fatal("first Write() succeeded, want error")
	}
	if err := s.Write(t.Context(), sampleRecords(1)); err != nil {
		t.Fatalf("second Write() = %v, want nil", err)
	}
	if got := bytes.Count(w.Bytes(), []byte("\n")); got != 1 {
		t.Fatalf("got %d lines, want exactly 1 (no partial data from the failed batch)", got)
	}
}
