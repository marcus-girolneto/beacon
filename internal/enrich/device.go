package enrich

import (
	"strings"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// maxUALength bounds how much of a User-Agent the classifier inspects.
// Legitimate UAs are well under this; longer ones are junk or abuse.
const maxUALength = 512

var (
	botTokens = [...]string{
		"bot", "crawl", "spider", "slurp", "headless", "lighthouse", "phantomjs",
		"curl/", "wget/", "python-requests", "python-urllib", "go-http-client",
		"httpclient", "okhttp", "facebookexternalhit", "preview", "monitor",
		"pingdom", "uptime",
	}
	tabletTokens = [...]string{"ipad", "tablet", "kindle", "silk/", "playbook", "nexus 7", "nexus 9", "sm-t"}
	mobileTokens = [...]string{
		"mobi", "iphone", "ipod", "android", "windows phone", "blackberry", "bb10",
		"opera mini", "iemobile",
	}
	desktopTokens = [...]string{"windows nt", "macintosh", "mac os x", "x11", "linux", "cros"}
)

// ClassifyDevice derives a coarse device class from a User-Agent string.
//
// It is a deliberately small substring classifier rather than a full UA
// database: marketing dashboards need desktop/mobile/tablet splits and bot
// filtering, not exact browser versions. Known limitation: iPadOS 13+ in
// desktop mode reports itself as Macintosh and is classified as desktop.
func ClassifyDevice(ua string) event.Device {
	if ua == "" {
		return event.DeviceUnknown
	}
	if len(ua) > maxUALength {
		ua = ua[:maxUALength]
	}
	ua = strings.ToLower(ua)

	switch {
	case containsAny(ua, botTokens[:]):
		return event.DeviceBot
	case containsAny(ua, tabletTokens[:]),
		strings.Contains(ua, "android") && !strings.Contains(ua, "mobile"):
		// Android tablets omit "Mobile" from the UA, per Google's guidance.
		return event.DeviceTablet
	case containsAny(ua, mobileTokens[:]):
		return event.DeviceMobile
	case containsAny(ua, desktopTokens[:]):
		return event.DeviceDesktop
	default:
		return event.DeviceUnknown
	}
}

func containsAny(s string, tokens []string) bool {
	for _, t := range tokens {
		if strings.Contains(s, t) {
			return true
		}
	}
	return false
}
