package enrich

import (
	"strings"
	"testing"

	"github.com/marcus-girolneto/beacon/internal/event"
)

func TestClassifyDevice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ua   string
		want event.Device
	}{
		{name: "empty", ua: "", want: event.DeviceUnknown},
		{name: "garbage", ua: "hello", want: event.DeviceUnknown},
		{name: "chrome windows", ua: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36", want: event.DeviceDesktop},
		{name: "safari macos", ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15", want: event.DeviceDesktop},
		{name: "firefox linux", ua: "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) Gecko/20100101 Firefox/131.0", want: event.DeviceDesktop},
		{name: "chromebook", ua: "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36", want: event.DeviceDesktop},
		{name: "iphone safari", ua: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1", want: event.DeviceMobile},
		{name: "android phone", ua: "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36", want: event.DeviceMobile},
		{name: "ipad legacy ua", ua: "Mozilla/5.0 (iPad; CPU OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1", want: event.DeviceTablet},
		{name: "android tablet", ua: "Mozilla/5.0 (Linux; Android 13; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36", want: event.DeviceTablet},
		{name: "kindle silk", ua: "Mozilla/5.0 (Linux; U; Android 4.0.3; KFTT Build/IML74K) AppleWebKit/537.36 (KHTML, like Gecko) Silk/3.68 like Chrome/39.0.2171.93 Safari/537.36", want: event.DeviceTablet},
		{name: "windows phone", ua: "Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Mobile Safari/537.36 Edge/15.14977", want: event.DeviceMobile},
		{name: "googlebot", ua: "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", want: event.DeviceBot},
		{name: "googlebot smartphone", ua: "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", want: event.DeviceBot},
		{name: "headless chrome", ua: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/130.0.0.0 Safari/537.36", want: event.DeviceBot},
		{name: "curl", ua: "curl/8.7.1", want: event.DeviceBot},
		{name: "go client", ua: "Go-http-client/1.1", want: event.DeviceBot},
		{name: "facebook preview", ua: "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)", want: event.DeviceBot},
		{name: "overlong ua is bounded", ua: "Mozilla/5.0 (Windows NT 10.0)" + strings.Repeat("x", 4096) + " bot", want: event.DeviceDesktop},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyDevice(tc.ua); got != tc.want {
				t.Errorf("ClassifyDevice(%q) = %q, want %q", tc.ua, got, tc.want)
			}
		})
	}
}

func BenchmarkClassifyDevice(b *testing.B) {
	ua := "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1"
	for b.Loop() {
		ClassifyDevice(ua)
	}
}
