package enrich

import (
	"net/url"
	"slices"
	"strings"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// a trailing "." matches any TLD (google. matches google.co.uk).
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

// ChannelInput is the input to ClassifyChannel. Hosts are lower-cased.
type ChannelInput struct {
	ReferrerHost string // empty means no referrer
	PageHost     string
	UTM          event.UTM
	PaidClick    bool
}

// ClassifyChannel picks a channel: utm_medium, then click IDs, then referrer.
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

// ReferrerHost returns the lower-cased referrer host or android-app package.
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
		// brand must start a label and be followed by a TLD
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
