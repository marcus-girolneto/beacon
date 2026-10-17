package enrich

import (
	"net/url"
	"slices"
	"strings"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// Domain lists used for referrer classification. An entry ending in "." is a
// brand pattern that matches any TLD (google. matches google.com and
// google.co.uk); other entries match the domain itself and its subdomains.
var (
	emailDomains = [...]string{
		"mail.google.com", "outlook.live.com", "outlook.office.com", "outlook.office365.com",
		"mail.yahoo.com", "mail.proton.me", "mail.aol.com", "com.google.android.gm",
		"com.microsoft.office.outlook",
	}
	searchDomains = [...]string{
		"google.", "bing.com", "duckduckgo.com", "yahoo.", "baidu.com", "yandex.",
		"ecosia.org", "search.brave.com", "startpage.com", "qwant.com", "kagi.com",
		"com.google.android.googlequicksearchbox",
	}
	socialDomains = [...]string{
		"facebook.com", "fb.com", "instagram.com", "t.co", "twitter.com", "x.com",
		"linkedin.com", "lnkd.in", "reddit.com", "youtube.com", "youtu.be",
		"pinterest.", "tiktok.com", "threads.net", "bsky.app", "mastodon.social",
		"news.ycombinator.com", "com.reddit.frontpage",
	}
)

var (
	paidMediums   = [...]string{"cpc", "ppc", "cpm", "cpv", "cpa", "paid", "paidsearch", "paid_search", "paid-search", "paidsocial", "paid_social", "paid-social", "display", "banner", "retargeting", "affiliate"}
	emailMediums  = [...]string{"email", "e-mail", "e_mail", "newsletter"}
	socialMediums = [...]string{"social", "social-network", "social_network", "social-media", "social_media", "sm"}
	searchMediums = [...]string{"organic", "search"}
)

// ChannelInput carries what ClassifyChannel needs to attribute an event.
type ChannelInput struct {
	// ReferrerHost is the lower-cased host of the referrer, or the package
	// name for android-app:// referrers. Empty means no referrer.
	ReferrerHost string
	// PageHost is the lower-cased host of the page the event happened on.
	PageHost string
	// UTM holds the normalized campaign parameters.
	UTM event.UTM
	// PaidClick reports whether the page URL carries an ad-platform click ID.
	PaidClick bool
}

// ClassifyChannel attributes an event to a marketing channel.
//
// Precedence follows what marketers expect from tools such as GA4: explicit
// campaign tagging (utm_medium) wins, then ad click IDs, then the referrer.
// Self-referrals (internal navigation) count as direct so that moving between
// pages does not overwrite the original source.
func ClassifyChannel(in ChannelInput) event.Channel {
	if ch, ok := channelFromMedium(in.UTM.Medium); ok {
		return ch
	}
	if in.PaidClick {
		return event.ChannelPaid
	}
	ref := strings.TrimPrefix(in.ReferrerHost, "www.")
	if ref == "" || ref == strings.TrimPrefix(in.PageHost, "www.") {
		if in.UTM.Source != "" {
			return event.ChannelReferral
		}
		return event.ChannelDirect
	}
	switch {
	case matchAny(ref, emailDomains[:]):
		return event.ChannelEmail
	case matchAny(ref, searchDomains[:]):
		return event.ChannelSearch
	case matchAny(ref, socialDomains[:]):
		return event.ChannelSocial
	default:
		return event.ChannelReferral
	}
}

func channelFromMedium(medium string) (event.Channel, bool) {
	switch {
	case medium == "":
		return "", false
	case slices.Contains(paidMediums[:], medium):
		return event.ChannelPaid, true
	case slices.Contains(emailMediums[:], medium):
		return event.ChannelEmail, true
	case slices.Contains(socialMediums[:], medium):
		return event.ChannelSocial, true
	case slices.Contains(searchMediums[:], medium):
		return event.ChannelSearch, true
	default:
		return "", false
	}
}

// ReferrerHost extracts the lower-cased host from a referrer URL. For
// android-app:// referrers it returns the app package name, which is how
// Android reports referrals from native apps such as Gmail.
func ReferrerHost(rawRef string) string {
	if rawRef == "" {
		return ""
	}
	u, err := url.Parse(rawRef)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

func matchAny(host string, patterns []string) bool {
	for _, p := range patterns {
		if matchDomain(host, p) {
			return true
		}
	}
	return false
}

func matchDomain(host, pattern string) bool {
	if brand, ok := strings.CutSuffix(pattern, "."); ok {
		// A brand label must be followed by at least one more label (the
		// TLD) and be either the first label or preceded by a dot.
		for i := 0; ; {
			j := strings.Index(host[i:], brand+".")
			if j < 0 {
				return false
			}
			j += i
			if (j == 0 || host[j-1] == '.') && j+len(brand)+1 < len(host) {
				return true
			}
			i = j + 1
		}
	}
	return host == pattern || strings.HasSuffix(host, "."+pattern)
}
