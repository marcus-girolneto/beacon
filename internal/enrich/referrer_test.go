package enrich

import (
	"testing"

	"github.com/marcus-girolneto/beacon/internal/event"
)

func TestClassifyChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   ChannelInput
		want event.Channel
	}{
		{name: "no referrer", in: ChannelInput{PageHost: "example.com"}, want: event.ChannelDirect},
		{name: "self referral", in: ChannelInput{ReferrerHost: "example.com", PageHost: "www.example.com"}, want: event.ChannelDirect},
		{name: "google", in: ChannelInput{ReferrerHost: "www.google.com"}, want: event.ChannelSearch},
		{name: "google ccTLD", in: ChannelInput{ReferrerHost: "www.google.co.uk"}, want: event.ChannelSearch},
		{name: "bing", in: ChannelInput{ReferrerHost: "www.bing.com"}, want: event.ChannelSearch},
		{name: "duckduckgo", in: ChannelInput{ReferrerHost: "duckduckgo.com"}, want: event.ChannelSearch},
		{name: "yahoo search", in: ChannelInput{ReferrerHost: "search.yahoo.co.jp"}, want: event.ChannelSearch},
		{name: "android google app", in: ChannelInput{ReferrerHost: "com.google.android.googlequicksearchbox"}, want: event.ChannelSearch},
		{name: "gmail web", in: ChannelInput{ReferrerHost: "mail.google.com"}, want: event.ChannelEmail},
		{name: "yahoo mail beats yahoo search", in: ChannelInput{ReferrerHost: "mail.yahoo.com"}, want: event.ChannelEmail},
		{name: "gmail android app", in: ChannelInput{ReferrerHost: "com.google.android.gm"}, want: event.ChannelEmail},
		{name: "facebook mobile", in: ChannelInput{ReferrerHost: "m.facebook.com"}, want: event.ChannelSocial},
		{name: "twitter shortener", in: ChannelInput{ReferrerHost: "t.co"}, want: event.ChannelSocial},
		{name: "linkedin", in: ChannelInput{ReferrerHost: "www.linkedin.com"}, want: event.ChannelSocial},
		{name: "pinterest ccTLD", in: ChannelInput{ReferrerHost: "br.pinterest.com"}, want: event.ChannelSocial},
		{name: "hacker news", in: ChannelInput{ReferrerHost: "news.ycombinator.com"}, want: event.ChannelSocial},
		{name: "unknown site", in: ChannelInput{ReferrerHost: "blog.golang.org"}, want: event.ChannelReferral},
		{name: "lookalike is not google", in: ChannelInput{ReferrerHost: "notgoogle.com"}, want: event.ChannelReferral},
		{name: "brand needs a tld", in: ChannelInput{ReferrerHost: "google"}, want: event.ChannelReferral},
		{name: "suffix lookalike is not t.co", in: ChannelInput{ReferrerHost: "reddit.co"}, want: event.ChannelReferral},
		{name: "utm medium cpc", in: ChannelInput{ReferrerHost: "www.google.com", UTM: event.UTM{Medium: "cpc"}}, want: event.ChannelPaid},
		{name: "utm medium paid_social", in: ChannelInput{ReferrerHost: "facebook.com", UTM: event.UTM{Medium: "paid_social"}}, want: event.ChannelPaid},
		{name: "utm medium email without referrer", in: ChannelInput{UTM: event.UTM{Source: "newsletter", Medium: "email"}}, want: event.ChannelEmail},
		{name: "utm medium social", in: ChannelInput{UTM: event.UTM{Medium: "social"}}, want: event.ChannelSocial},
		{name: "utm medium organic", in: ChannelInput{UTM: event.UTM{Medium: "organic"}}, want: event.ChannelSearch},
		{name: "unknown medium falls back to referrer", in: ChannelInput{ReferrerHost: "t.co", UTM: event.UTM{Medium: "launch"}}, want: event.ChannelSocial},
		{name: "click id without utm", in: ChannelInput{ReferrerHost: "www.google.com", PaidClick: true}, want: event.ChannelPaid},
		{name: "source only without referrer", in: ChannelInput{UTM: event.UTM{Source: "partner"}}, want: event.ChannelReferral},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyChannel(tc.in); got != tc.want {
				t.Errorf("ClassifyChannel(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestReferrerHost(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"":                                       "",
		"https://WWW.Google.com/search?q=go":     "www.google.com",
		"https://example.com:8443/a":             "example.com",
		"android-app://com.google.android.gm/":   "com.google.android.gm",
		"https://exa mple.com/%zz":               "",
		"http://user:pass@news.ycombinator.com/": "news.ycombinator.com",
	}
	for in, want := range tests {
		if got := ReferrerHost(in); got != want {
			t.Errorf("ReferrerHost(%q) = %q, want %q", in, got, want)
		}
	}
}
