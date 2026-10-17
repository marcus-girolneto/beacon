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

// ErrInvalid matches every validation error.
var ErrInvalid = errors.New("invalid event")

// Field length limits.
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

// FieldError is one invalid field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError lists the invalid fields of an event.
type ValidationError struct {
	Fields []FieldError
}

// Error implements error.
func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "invalid event: " + strings.Join(parts, "; ")
}

// Is matches ErrInvalid.
func (e *ValidationError) Is(target error) bool {
	return target == ErrInvalid
}

// Validator checks events. Use NewValidator, the zero value is not usable.
type Validator struct {
	MaxPropertiesBytes int
	MaxAge             time.Duration
	MaxClockSkew       time.Duration // how far in the future a timestamp may be
	Now                func() time.Time
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

// Validate returns a *ValidationError listing every problem, or nil.
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

func isJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}'
}
