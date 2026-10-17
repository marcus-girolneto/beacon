package enrich

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/marcus-girolneto/beacon/internal/event"
)

func TestParseUTM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want event.UTM
	}{
		{name: "no query", url: "https://example.com/pricing", want: event.UTM{}},
		{name: "query without utm", url: "https://example.com/?a=1&b=2", want: event.UTM{}},
		{
			name: "all five params",
			url:  "https://example.com/?utm_source=Google&utm_medium=CPC&utm_campaign=Spring_Sale&utm_term=go+jobs&utm_content=ad%201",
			want: event.UTM{Source: "google", Medium: "cpc", Campaign: "Spring_Sale", Term: "go jobs", Content: "ad 1"},
		},
		{
			name: "mixed with other params",
			url:  "https://example.com/p?ref=x&utm_campaign=launch&id=7",
			want: event.UTM{Campaign: "launch"},
		},
		{
			name: "fragment is ignored",
			url:  "https://example.com/?utm_source=newsletter#utm_medium=email",
			want: event.UTM{Source: "newsletter"},
		},
		{
			name: "first value wins on duplicates",
			url:  "https://example.com/?utm_source=a&utm_source=b",
			want: event.UTM{Source: "a"},
		},
		{
			name: "whitespace trimmed",
			url:  "https://example.com/?utm_source=%20%20twitter%20",
			want: event.UTM{Source: "twitter"},
		},
		{
			name: "control characters removed",
			url:  "https://example.com/?utm_campaign=fall%0Asale%00",
			want: event.UTM{Campaign: "fallsale"},
		},
		{
			name: "invalid utf8 removed",
			url:  "https://example.com/?utm_campaign=caf%C3%A9%FF",
			want: event.UTM{Campaign: "café"},
		},
		{
			name: "malformed escape keeps other pairs",
			url:  "https://example.com/?utm_source=bing&utm_term=%zz&utm_medium=cpc",
			want: event.UTM{Source: "bing", Medium: "cpc"},
		},
		{
			name: "overlong value truncated",
			url:  "https://example.com/?utm_campaign=" + strings.Repeat("x", event.MaxUTMLength+50),
			want: event.UTM{Campaign: strings.Repeat("x", event.MaxUTMLength)},
		},
		{name: "relative url", url: "/landing?utm_source=fb", want: event.UTM{Source: "fb"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseUTM(tc.url); got != tc.want {
				t.Errorf("ParseUTM(%q) = %+v, want %+v", tc.url, got, tc.want)
			}
		})
	}
}

func TestTruncateKeepsRunesIntact(t *testing.T) {
	t.Parallel()
	s := strings.Repeat("é", 10)
	got := truncate(s, 5)
	if got != "éé" {
		t.Errorf("truncate() = %q, want %q", got, "éé")
	}
	if got := truncate("abc", 5); got != "abc" {
		t.Errorf("truncate() = %q, want %q", got, "abc")
	}
}

func TestNormalizeUTM(t *testing.T) {
	t.Parallel()
	got := NormalizeUTM(event.UTM{Source: " Newsletter ", Medium: "EMAIL", Campaign: " Q4 "})
	want := event.UTM{Source: "newsletter", Medium: "email", Campaign: "Q4"}
	if got != want {
		t.Errorf("NormalizeUTM() = %+v, want %+v", got, want)
	}
}

func TestHasPaidClickID(t *testing.T) {
	t.Parallel()
	tests := map[string]bool{
		"https://example.com/":                     false,
		"https://example.com/?gclid=abc":           true,
		"https://example.com/?x=1&msclkid=abc":     true,
		"https://example.com/?fbclid=abc":          false,
		"https://example.com/?notgclid=abc":        false,
		"https://example.com/?ref=a&ttclid=t&z=1":  true,
		"https://example.com/?utm_source=google":   false,
		"https://example.com/?li_fat_id=1&gclid=2": true,
	}
	for u, want := range tests {
		if got := hasPaidClickID(u); got != want {
			t.Errorf("hasPaidClickID(%q) = %v, want %v", u, got, want)
		}
	}
}

func FuzzParseUTM(f *testing.F) {
	seeds := []string{
		"https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign=spring",
		"https://example.com/?utm_term=%zz&utm_content=%",
		"?utm_source=" + strings.Repeat("é", 200),
		"https://example.com/?utm_campaign=a%00b%0Ac",
		"https://example.com/?utm_source=;;&&==utm_medium",
		"no-query-at-all",
		"",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		got := ParseUTM(raw)
		for name, v := range map[string]string{
			"source": got.Source, "medium": got.Medium, "campaign": got.Campaign,
			"term": got.Term, "content": got.Content,
		} {
			if len(v) > event.MaxUTMLength {
				t.Fatalf("%s has %d bytes, exceeds limit", name, len(v))
			}
			if !utf8.ValidString(v) {
				t.Fatalf("%s is not valid UTF-8: %q", name, v)
			}
			if strings.ContainsFunc(v, unicode.IsControl) {
				t.Fatalf("%s contains control characters: %q", name, v)
			}
			if v != strings.TrimSpace(v) {
				t.Fatalf("%s is not trimmed: %q", name, v)
			}
		}
		if got.Source != strings.ToLower(got.Source) || got.Medium != strings.ToLower(got.Medium) {
			t.Fatalf("source/medium not lower-cased: %+v", got)
		}
		if !strings.Contains(raw, "utm_") && !got.IsZero() {
			t.Fatalf("ParseUTM(%q) = %+v, want zero for input without utm_", raw, got)
		}
		v := event.NewValidator(nil)
		e := event.Event{Type: event.TypePageview, AnonymousID: "a", URL: "https://x.io/", UTM: got}
		if err := v.Validate(&e); err != nil {
			t.Fatalf("parsed UTM fails validation: %v", err)
		}
	})
}
