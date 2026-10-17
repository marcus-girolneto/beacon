// Package enrich turns validated client events into privacy-safe records:
// it extracts UTM parameters, attributes each event to an acquisition
// channel, derives a coarse device class and replaces client identifiers
// with a daily-rotating, non-reversible visitor hash.
package enrich

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// ParseUTM extracts utm_* query parameters from rawURL. It never fails:
// malformed URLs or query strings yield whatever could be parsed, which may
// be nothing. Source and medium are lower-cased so that "Google" and
// "google" aggregate together; every value is trimmed, stripped of control
// characters and truncated to event.MaxUTMLength bytes.
func ParseUTM(rawURL string) event.UTM {
	_, query, ok := strings.Cut(rawURL, "?")
	if !ok || !strings.Contains(query, "utm_") {
		return event.UTM{}
	}
	query, _, _ = strings.Cut(query, "#")

	// ParseQuery returns every pair it managed to decode alongside the first
	// error, and partial attribution beats none.
	values, _ := url.ParseQuery(query)
	return event.UTM{
		Source:   clean(values.Get("utm_source"), true),
		Medium:   clean(values.Get("utm_medium"), true),
		Campaign: clean(values.Get("utm_campaign"), false),
		Term:     clean(values.Get("utm_term"), false),
		Content:  clean(values.Get("utm_content"), false),
	}
}

// NormalizeUTM applies the same normalization as ParseUTM to values that a
// client supplied explicitly.
func NormalizeUTM(u event.UTM) event.UTM {
	return event.UTM{
		Source:   clean(u.Source, true),
		Medium:   clean(u.Medium, true),
		Campaign: clean(u.Campaign, false),
		Term:     clean(u.Term, false),
		Content:  clean(u.Content, false),
	}
}

func clean(s string, lower bool) string {
	if s == "" {
		return ""
	}
	s = strings.ToValidUTF8(s, "")
	if strings.ContainsFunc(s, unicode.IsControl) {
		s = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return -1
			}
			return r
		}, s)
	}
	if lower {
		// Lower-casing can change a string's byte length, so it must
		// happen before truncation.
		s = strings.ToLower(s)
	}
	return strings.TrimSpace(truncate(strings.TrimSpace(s), event.MaxUTMLength))
}

// truncate shortens s to at most n bytes without splitting a rune.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// paidClickIDs are query parameters appended by ad platforms to landing page
// URLs. Their presence identifies paid traffic even when UTM tags are missing.
// fbclid is deliberately absent: Facebook appends it to organic outbound
// links too, so it does not imply a paid click.
var paidClickIDs = [...]string{"gclid=", "gbraid=", "wbraid=", "msclkid=", "ttclid=", "li_fat_id=", "twclid="}

// hasPaidClickID reports whether rawURL carries an ad-platform click ID.
func hasPaidClickID(rawURL string) bool {
	_, query, ok := strings.Cut(rawURL, "?")
	if !ok {
		return false
	}
	for _, id := range paidClickIDs {
		if i := strings.Index(query, id); i == 0 || (i > 0 && query[i-1] == '&') {
			return true
		}
	}
	return false
}
