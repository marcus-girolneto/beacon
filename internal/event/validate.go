package event

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ErrInvalid is matched by every validation error returned from Validate.
var ErrInvalid = errors.New("invalid event")

// Field length limits enforced by the Validator.
const (
	MaxIDLength       = 128
	MaxNameLength     = 128
	MaxURLLength      = 2048
	MaxUTMLength      = 256
	DefaultMaxProps   = 8 << 10
	DefaultMaxAge     = 7 * 24 * time.Hour
	DefaultMaxSkew    = 5 * time.Minute
	maxMessageIDBytes = 64
)

// FieldError describes why a single field is invalid.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError lists every invalid field of an event.
type ValidationError struct {
	Fields []FieldError
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid event: " + strings.Join(parts, "; ")
}

// Is makes errors.Is(err, ErrInvalid) true for validation errors.
func (e *ValidationError) Is(target error) bool {
	return target == ErrInvalid
}

// Validator checks events against size, format and freshness rules.
// The zero value is not usable; construct it with NewValidator.
type Validator struct {
	// MaxPropertiesBytes bounds the serialized size of Event.Properties.
	MaxPropertiesBytes int
	// MaxAge rejects events whose timestamp is older than now minus MaxAge.
	MaxAge time.Duration
	// MaxClockSkew rejects events whose timestamp is more than MaxClockSkew
	// in the future, which almost always means a broken client clock.
	MaxClockSkew time.Duration
	// Now returns the current time.
	Now func() time.Time
}

// NewValidator returns a Validator with default limits.
func NewValidator(now func() time.Time) Validator {
	if now == nil {
		now = time.Now
	}
	return Validator{
		MaxPropertiesBytes: DefaultMaxProps,
		MaxAge:             DefaultMaxAge,
		MaxClockSkew:       DefaultMaxSkew,
		Now:                now,
	}
}

// Validate reports every rule e violates as a *ValidationError, or nil if e
// is acceptable.
func (v Validator) Validate(e *Event) error {
	var errs []FieldError
	add := func(field, format string, args ...any) {
		errs = append(errs, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
	}

	switch {
	case e.Type == "":
		add("type", "is required")
	case !e.Type.Valid():
		add("type", "must be one of pageview, click, custom")
	}

	if e.Type == TypeCustom && e.Name == "" {
		add("name", "is required for custom events")
	}
	if msg := checkText(e.Name, MaxNameLength); msg != "" {
		add("name", "%s", msg)
	}

	if e.AnonymousID == "" {
		add("anonymous_id", "is required")
	} else if msg := checkText(e.AnonymousID, MaxIDLength); msg != "" {
		add("anonymous_id", "%s", msg)
	}

	if msg := checkText(e.MessageID, maxMessageIDBytes); msg != "" {
		add("message_id", "%s", msg)
	}

	if e.URL == "" {
		add("url", "is required")
	} else if msg := checkURL(e.URL, true); msg != "" {
		add("url", "%s", msg)
	}
	if e.Referrer != "" {
		if msg := checkURL(e.Referrer, false); msg != "" {
			add("referrer", "%s", msg)
		}
	}

	for _, f := range [...]struct{ name, val string }{
		{"utm.source", e.UTM.Source},
		{"utm.medium", e.UTM.Medium},
		{"utm.campaign", e.UTM.Campaign},
		{"utm.term", e.UTM.Term},
		{"utm.content", e.UTM.Content},
	} {
		if msg := checkText(f.val, MaxUTMLength); msg != "" {
			add(f.name, "%s", msg)
		}
	}

	if len(e.Properties) > 0 {
		switch {
		case len(e.Properties) > v.MaxPropertiesBytes:
			add("properties", "must not exceed %d bytes", v.MaxPropertiesBytes)
		case !isJSONObject(e.Properties):
			add("properties", "must be a JSON object")
		}
	}

	if !e.Timestamp.IsZero() {
		now := v.Now()
		switch {
		case e.Timestamp.After(now.Add(v.MaxClockSkew)):
			add("timestamp", "is more than %s in the future", v.MaxClockSkew)
		case e.Timestamp.Before(now.Add(-v.MaxAge)):
			add("timestamp", "is older than %s", v.MaxAge)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return &ValidationError{Fields: errs}
}

func checkText(s string, maxLen int) string {
	if len(s) > maxLen {
		return fmt.Sprintf("must not exceed %d bytes", maxLen)
	}
	if !utf8.ValidString(s) {
		return "must be valid UTF-8"
	}
	if strings.ContainsFunc(s, unicode.IsControl) {
		return "must not contain control characters"
	}
	return ""
}

func checkURL(raw string, requireHTTP bool) string {
	if len(raw) > MaxURLLength {
		return fmt.Sprintf("must not exceed %d bytes", MaxURLLength)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "must be a valid URL"
	}
	if requireHTTP {
		if u.Scheme != "http" && u.Scheme != "https" {
			return "must be an absolute http(s) URL"
		}
		if u.Host == "" {
			return "must include a host"
		}
		return ""
	}
	if u.Scheme == "" {
		return "must be an absolute URL"
	}
	return ""
}

// isJSONObject reports whether raw looks like a JSON object. The decoder has
// already guaranteed raw is well-formed JSON, so checking the delimiter is
// sufficient.
func isJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}'
}
