package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/marcus-girolneto/beacon/internal/enrich"
	"github.com/marcus-girolneto/beacon/internal/event"
	"github.com/marcus-girolneto/beacon/internal/pipeline"
)

// Rejection reasons reported to metrics.
const (
	reasonMalformed   = "malformed"
	reasonInvalid     = "invalid"
	reasonTooLarge    = "too_large"
	reasonContentType = "content_type"
	reasonInternal    = "internal"
)

// Endpoint labels reported to metrics.
const (
	endpointTrack = "track"
	endpointBatch = "batch"
	endpointPixel = "pixel"
)

// transparentGIF is the smallest valid 1x1 transparent GIF.
var transparentGIF = []byte{
	0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
	0xff, 0xff, 0xff, 0x21, 0xf9, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
}

type acceptedResponse struct {
	Accepted int             `json:"accepted"`
	Rejected []batchRejected `json:"rejected,omitempty"`
}

type batchRejected struct {
	Index  int                `json:"index"`
	Fields []event.FieldError `json:"fields"`
}

type batchRequest struct {
	Batch []event.Event `json:"batch"`
}

// errorResponse is the body of every 4xx/5xx response.
type errorResponse struct {
	Error  string             `json:"error"`
	Fields []event.FieldError `json:"fields,omitempty"`
}

func (s *Server) handleTrack(w http.ResponseWriter, r *http.Request) {
	var e event.Event
	if !s.decodeBody(w, r, s.cfg.MaxEventBytes, &e) {
		return
	}
	if err := s.validator.Validate(&e); err != nil {
		s.rec.EventsRejected(reasonInvalid, 1)
		writeValidationError(w, err)
		return
	}
	rec, err := s.enricher.Enrich(&e, s.requestMeta(r))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !s.enqueue(w, r, endpointTrack, rec) {
		return
	}
	writeJSON(w, http.StatusAccepted, acceptedResponse{Accepted: 1})
}

// handleBatch accepts up to MaxBatchEvents events. Invalid events are
// reported by index and skipped; the valid remainder is admitted as a unit,
// so a client that gets a 503 can safely retry the whole request without
// creating duplicates.
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !s.decodeBody(w, r, s.cfg.MaxBatchBytes, &req) {
		return
	}
	switch n := len(req.Batch); {
	case n == 0:
		s.rec.EventsRejected(reasonMalformed, 1)
		writeError(w, http.StatusBadRequest, "batch must contain at least one event")
		return
	case n > s.cfg.MaxBatchEvents:
		s.rec.EventsRejected(reasonTooLarge, n)
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("batch must not exceed %d events", s.cfg.MaxBatchEvents))
		return
	}

	meta := s.requestMeta(r)
	recs := make([]event.Record, 0, len(req.Batch))
	var rejected []batchRejected
	for i := range req.Batch {
		e := &req.Batch[i]
		if err := s.validator.Validate(e); err != nil {
			rejected = append(rejected, batchRejected{Index: i, Fields: fieldErrors(err)})
			continue
		}
		rec, err := s.enricher.Enrich(e, meta)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		recs = append(recs, rec)
	}
	if len(rejected) > 0 {
		s.rec.EventsRejected(reasonInvalid, len(rejected))
	}
	if len(recs) == 0 {
		writeJSON(w, http.StatusBadRequest, acceptedResponse{Rejected: rejected})
		return
	}
	if !s.enqueue(w, r, endpointBatch, recs...) {
		return
	}
	writeJSON(w, http.StatusAccepted, acceptedResponse{Accepted: len(recs), Rejected: rejected})
}

// handlePixel records an event described by query parameters and always
// answers with a 1x1 GIF, so a failed track never shows a broken image. The
// status code still reflects the outcome for debugging.
//
// Parameters: t (type, default pageview), n (name), aid (anonymous id),
// url (default: Referer header), ref, utm_source, utm_medium,
// utm_campaign, utm_term, utm_content.
func (s *Server) handlePixel(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	e := event.Event{
		Type:        event.Type(q.Get("t")),
		Name:        q.Get("n"),
		AnonymousID: q.Get("aid"),
		URL:         q.Get("url"),
		Referrer:    q.Get("ref"),
		UTM: event.UTM{
			Source:   q.Get("utm_source"),
			Medium:   q.Get("utm_medium"),
			Campaign: q.Get("utm_campaign"),
			Term:     q.Get("utm_term"),
			Content:  q.Get("utm_content"),
		},
	}
	if e.Type == "" {
		e.Type = event.TypePageview
	}
	if e.URL == "" {
		e.URL = r.Referer()
	}

	status := http.StatusOK
	if err := s.validator.Validate(&e); err != nil {
		s.rec.EventsRejected(reasonInvalid, 1)
		status = http.StatusBadRequest
	} else if rec, err := s.enricher.Enrich(&e, s.requestMeta(r)); err != nil {
		s.log.ErrorContext(r.Context(), "enrich pixel event", "err", err)
		s.rec.EventsRejected(reasonInternal, 1)
		status = http.StatusInternalServerError
	} else if err := s.queue.Enqueue(r.Context(), rec); err != nil {
		status = s.shedStatus(w, err, 1)
	} else {
		s.rec.EventsAccepted(endpointPixel, 1)
	}

	h := w.Header()
	h.Set("Content-Type", "image/gif")
	h.Set("Content-Length", strconv.Itoa(len(transparentGIF)))
	h.Set("Cache-Control", "no-store, max-age=0")
	w.WriteHeader(status)
	_, _ = w.Write(transparentGIF)
}

func (s *Server) requestMeta(r *http.Request) enrich.RequestMeta {
	return enrich.RequestMeta{
		IP:         s.clientIP(r),
		UserAgent:  r.UserAgent(),
		ReceivedAt: s.cfg.Now(),
	}
}

// enqueue admits recs and writes the error response if that fails.
func (s *Server) enqueue(w http.ResponseWriter, r *http.Request, endpoint string, recs ...event.Record) bool {
	err := s.queue.Enqueue(r.Context(), recs...)
	if err == nil {
		s.rec.EventsAccepted(endpoint, len(recs))
		return true
	}
	status := s.shedStatus(w, err, len(recs))
	msg := "event buffer is full, retry later"
	switch status {
	case http.StatusRequestEntityTooLarge:
		msg = "batch exceeds buffer capacity"
	case http.StatusServiceUnavailable:
		if errors.Is(err, pipeline.ErrClosed) {
			msg = "server is shutting down, retry later"
		}
	default:
		msg = "request canceled"
	}
	writeError(w, status, msg)
	return false
}

// shedStatus maps an Enqueue error to a status code, sets Retry-After where
// appropriate and records the drop.
func (s *Server) shedStatus(w http.ResponseWriter, err error, n int) int {
	switch {
	case errors.Is(err, pipeline.ErrBufferFull):
		s.rec.EventsDropped("buffer_full", n)
	case errors.Is(err, pipeline.ErrClosed):
		s.rec.EventsDropped("shutting_down", n)
	case errors.Is(err, pipeline.ErrTooLarge):
		s.rec.EventsRejected(reasonTooLarge, n)
		return http.StatusRequestEntityTooLarge
	default:
		// The client went away while waiting for buffer space.
		s.rec.EventsDropped("canceled", n)
		return 499
	}
	w.Header().Set("Retry-After", strconv.Itoa(max(int(s.cfg.RetryAfter.Seconds()), 1)))
	return http.StatusServiceUnavailable
}

// decodeBody strictly decodes a single JSON value from the request body into
// dst and writes the error response if that fails.
func (s *Server) decodeBody(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	if !acceptableContentType(r.Header.Get("Content-Type")) {
		s.rec.EventsRejected(reasonContentType, 1)
		writeError(w, http.StatusUnsupportedMediaType, "content type must be application/json or text/plain")
		return false
	}

	body := http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil {
		// Reject trailing data such as a second JSON value.
		if _, terr := dec.Token(); !errors.Is(terr, io.EOF) {
			err = errors.New("unexpected data after JSON value")
		}
	}
	if err == nil {
		return true
	}

	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		s.rec.EventsRejected(reasonTooLarge, 1)
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must not exceed %d bytes", maxErr.Limit))
		return false
	}
	s.rec.EventsRejected(reasonMalformed, 1)
	writeError(w, http.StatusBadRequest, "malformed JSON: "+jsonErrorMessage(err))
	return false
}

// acceptableContentType allows application/json and, for navigator.sendBeacon
// which cannot set JSON without a CORS preflight, text/plain.
func acceptableContentType(ct string) bool {
	if ct == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return mt == "application/json" || mt == "text/plain"
}

func jsonErrorMessage(err error) string {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &syntaxErr):
		return fmt.Sprintf("syntax error at offset %d", syntaxErr.Offset)
	case errors.As(err, &typeErr):
		return fmt.Sprintf("field %q must be %s", typeErr.Field, typeErr.Type)
	case errors.Is(err, io.EOF):
		return "empty body"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected end of input"
	default:
		return err.Error()
	}
}

func fieldErrors(err error) []event.FieldError {
	var verr *event.ValidationError
	if errors.As(err, &verr) {
		return verr.Fields
	}
	return []event.FieldError{{Field: "", Message: err.Error()}}
}

func writeValidationError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid event", Fields: fieldErrors(err)})
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "path", r.URL.Path, "err", err)
	s.rec.EventsRejected(reasonInternal, 1)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}
