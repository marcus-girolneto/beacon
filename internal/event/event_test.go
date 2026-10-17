package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marcus-girolneto/beacon/internal/event"
)

var fixedNow = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func validEvent() event.Event {
	return event.Event{
		Type:        event.TypePageview,
		AnonymousID: "anon-123",
		URL:         "https://example.com/pricing?utm_source=google",
		Referrer:    "https://www.google.com/",
		Timestamp:   fixedNow.Add(-time.Minute),
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(e *event.Event)
		wantFields []string
	}{
		{name: "valid pageview", mutate: func(*event.Event) {}},
		{name: "valid click without referrer", mutate: func(e *event.Event) { e.Type = event.TypeClick; e.Referrer = "" }},
		{name: "valid custom with name", mutate: func(e *event.Event) { e.Type = event.TypeCustom; e.Name = "signup" }},
		{name: "zero timestamp is allowed", mutate: func(e *event.Event) { e.Timestamp = time.Time{} }},
		{name: "properties object", mutate: func(e *event.Event) { e.Properties = json.RawMessage(` {"plan":"pro"} `) }},
		{name: "android-app referrer", mutate: func(e *event.Event) { e.Referrer = "android-app://com.google.android.gm" }},
		{name: "missing type", mutate: func(e *event.Event) { e.Type = "" }, wantFields: []string{"type"}},
		{name: "unknown type", mutate: func(e *event.Event) { e.Type = "purchase" }, wantFields: []string{"type"}},
		{name: "custom without name", mutate: func(e *event.Event) { e.Type = event.TypeCustom }, wantFields: []string{"name"}},
		{name: "name too long", mutate: func(e *event.Event) { e.Name = strings.Repeat("n", event.MaxNameLength+1) }, wantFields: []string{"name"}},
		{name: "missing anonymous_id", mutate: func(e *event.Event) { e.AnonymousID = "" }, wantFields: []string{"anonymous_id"}},
		{name: "anonymous_id too long", mutate: func(e *event.Event) { e.AnonymousID = strings.Repeat("a", event.MaxIDLength+1) }, wantFields: []string{"anonymous_id"}},
		{name: "anonymous_id control char", mutate: func(e *event.Event) { e.AnonymousID = "abc\x00" }, wantFields: []string{"anonymous_id"}},
		{name: "anonymous_id invalid utf8", mutate: func(e *event.Event) { e.AnonymousID = "abc\xff" }, wantFields: []string{"anonymous_id"}},
		{name: "message_id too long", mutate: func(e *event.Event) { e.MessageID = strings.Repeat("m", 65) }, wantFields: []string{"message_id"}},
		{name: "missing url", mutate: func(e *event.Event) { e.URL = "" }, wantFields: []string{"url"}},
		{name: "relative url", mutate: func(e *event.Event) { e.URL = "/pricing" }, wantFields: []string{"url"}},
		{name: "non-http url", mutate: func(e *event.Event) { e.URL = "ftp://example.com/x" }, wantFields: []string{"url"}},
		{name: "url without host", mutate: func(e *event.Event) { e.URL = "https:///path" }, wantFields: []string{"url"}},
		{name: "unparseable url", mutate: func(e *event.Event) { e.URL = "https://exa mple.com/%zz" }, wantFields: []string{"url"}},
		{name: "url too long", mutate: func(e *event.Event) { e.URL = "https://example.com/" + strings.Repeat("p", event.MaxURLLength) }, wantFields: []string{"url"}},
		{name: "relative referrer", mutate: func(e *event.Event) { e.Referrer = "google.com" }, wantFields: []string{"referrer"}},
		{name: "referrer too long", mutate: func(e *event.Event) { e.Referrer = "https://a.com/" + strings.Repeat("r", event.MaxURLLength) }, wantFields: []string{"referrer"}},
		{name: "utm too long", mutate: func(e *event.Event) { e.UTM.Campaign = strings.Repeat("c", event.MaxUTMLength+1) }, wantFields: []string{"utm.campaign"}},
		{name: "utm control char", mutate: func(e *event.Event) { e.UTM.Source = "news\nletter" }, wantFields: []string{"utm.source"}},
		{name: "properties array", mutate: func(e *event.Event) { e.Properties = json.RawMessage(`[1,2]`) }, wantFields: []string{"properties"}},
		{name: "properties too large", mutate: func(e *event.Event) {
			e.Properties = json.RawMessage(`{"k":"` + strings.Repeat("x", event.DefaultMaxProps) + `"}`)
		}, wantFields: []string{"properties"}},
		{name: "timestamp in future", mutate: func(e *event.Event) { e.Timestamp = fixedNow.Add(time.Hour) }, wantFields: []string{"timestamp"}},
		{name: "timestamp within skew", mutate: func(e *event.Event) { e.Timestamp = fixedNow.Add(time.Minute) }},
		{name: "timestamp too old", mutate: func(e *event.Event) { e.Timestamp = fixedNow.Add(-8 * 24 * time.Hour) }, wantFields: []string{"timestamp"}},
		{name: "multiple errors reported together", mutate: func(e *event.Event) {
			e.Type = ""
			e.AnonymousID = ""
			e.URL = ""
		}, wantFields: []string{"type", "anonymous_id", "url"}},
	}

	v := event.NewValidator(func() time.Time { return fixedNow })
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := validEvent()
			tc.mutate(&e)

			err := v.Validate(&e)
			if len(tc.wantFields) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, event.ErrInvalid) {
				t.Fatalf("Validate() = %v, want error matching ErrInvalid", err)
			}
			var verr *event.ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("Validate() error type = %T, want *ValidationError", err)
			}
			got := make([]string, len(verr.Fields))
			for i, f := range verr.Fields {
				got[i] = f.Field
			}
			if strings.Join(got, ",") != strings.Join(tc.wantFields, ",") {
				t.Errorf("invalid fields = %v, want %v (%v)", got, tc.wantFields, err)
			}
		})
	}
}

func TestValidationErrorMessage(t *testing.T) {
	t.Parallel()
	err := &event.ValidationError{Fields: []event.FieldError{
		{Field: "type", Message: "is required"},
		{Field: "url", Message: "is required"},
	}}
	want := "invalid event: type: is required; url: is required"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestNewValidatorDefaultsClock(t *testing.T) {
	t.Parallel()
	v := event.NewValidator(nil)
	e := validEvent()
	e.Timestamp = time.Now()
	if err := v.Validate(&e); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestUTMMerge(t *testing.T) {
	t.Parallel()
	explicit := event.UTM{Source: "newsletter", Campaign: "fall"}
	parsed := event.UTM{Source: "google", Medium: "cpc", Campaign: "spring", Term: "go", Content: "ad1"}

	got := explicit.Merge(parsed)
	want := event.UTM{Source: "newsletter", Medium: "cpc", Campaign: "fall", Term: "go", Content: "ad1"}
	if got != want {
		t.Errorf("Merge() = %+v, want %+v", got, want)
	}
	if !(event.UTM{}).IsZero() || got.IsZero() {
		t.Error("IsZero() mismatch")
	}
}

func TestTypeValid(t *testing.T) {
	t.Parallel()
	for _, typ := range []event.Type{event.TypePageview, event.TypeClick, event.TypeCustom} {
		if !typ.Valid() {
			t.Errorf("%q.Valid() = false, want true", typ)
		}
	}
	if event.Type("identify").Valid() {
		t.Error(`"identify".Valid() = true, want false`)
	}
}
