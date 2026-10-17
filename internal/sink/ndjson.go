// Package sink provides pipeline sinks that serialize records as
// newline-delimited JSON, either to an arbitrary writer such as stdout or to
// an append-only file.
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// NDJSON writes each record as one JSON line. Writes are serialized, so a
// single NDJSON can be shared by several batchers without lines from
// concurrent batches interleaving.
type NDJSON struct {
	mu  sync.Mutex
	out io.Writer
	w   *bufio.Writer
	enc *json.Encoder
}

// NewNDJSON returns a sink that writes to w.
func NewNDJSON(w io.Writer) *NDJSON {
	bw := bufio.NewWriterSize(w, 64<<10)
	return &NDJSON{out: w, w: bw, enc: json.NewEncoder(bw)}
}

// Write encodes recs and flushes them to the underlying writer.
func (s *NDJSON) Write(ctx context.Context, recs []event.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range recs {
		if err := s.enc.Encode(&recs[i]); err != nil {
			s.w.Reset(s.out)
			return fmt.Errorf("encode record: %w", err)
		}
	}
	if err := s.w.Flush(); err != nil {
		// bufio.Writer errors are sticky; reset so a transient failure does
		// not poison every later batch. The retry rewrites the whole batch.
		s.w.Reset(s.out)
		return fmt.Errorf("flush ndjson: %w", err)
	}
	return nil
}

// File is an NDJSON sink backed by an append-only file. Each successful
// Write is fsynced, so acknowledged batches survive a crash of the host.
type File struct {
	*NDJSON
	f *os.File
}

// OpenFile opens (creating if needed) path for appending.
func OpenFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // path is operator configuration
	if err != nil {
		return nil, fmt.Errorf("open sink file: %w", err)
	}
	return &File{NDJSON: NewNDJSON(f), f: f}, nil
}

// Write appends recs to the file and syncs it to stable storage.
func (s *File) Write(ctx context.Context, recs []event.Record) error {
	if err := s.NDJSON.Write(ctx, recs); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("sync sink file: %w", err)
	}
	return nil
}

// Close flushes buffered data and closes the file.
func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.w.Flush(), s.f.Close())
}
