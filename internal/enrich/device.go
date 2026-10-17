package enrich

import (
	"strings"

	"github.com/marcus-girolneto/beacon/internal/event"
)

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

// ClassifyDevice derives a coarse device class from a User-Agent.
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
		// android tablets omit "Mobile"
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
