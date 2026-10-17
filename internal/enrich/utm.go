// Package enrich turns validated events into privacy-safe records.
package enrich

import (
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// ParseUTM extracts normalized utm_* parameters from rawURL. It never fails.
func ParseUTM(rawURL string) event.UTM {
	_, query, ok := strings.Cut(rawURL, "?")
	if !ok || !strings.Contains(query, "utm_") {
		return event.UTM{}
	}
	query, _, _ = strings.Cut(query, "#")

	// partial results beat none
	values, _ := url.ParseQuery(query)
	return event.UTM{
		Source:   clean(values.Get("utm_source"), true),
		Medium:   clean(values.Get("utm_medium"), true),
		Campaign: clean(values.Get("utm_campaign"), false),
		Term:     clean(values.Get("utm_term"), false),
		Content:  clean(values.Get("utm_content"), false),
	}
}

// NormalizeUTM normalizes client-supplied values like ParseUTM does.
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
		// before truncate, lowering can change byte length
		s = strings.ToLower(s)
	}
	return strings.TrimSpace(truncate(strings.TrimSpace(s), event.MaxUTMLength))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// no fbclid, facebook adds it to organic links too.
var paidClickIDs = [...]string{"gclid=", "gbraid=", "wbraid=", "msclkid=", "ttclid=", "li_fat_id=", "twclid="}

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
