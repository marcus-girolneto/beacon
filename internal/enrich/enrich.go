package enrich

import (
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/marcus-girolneto/beacon/internal/event"
)

// RequestMeta carries the transport-level attributes of the request that
// delivered an event. They are used for enrichment and then discarded.
type RequestMeta struct {
	IP         string
	UserAgent  string
	ReceivedAt time.Time
}

// Enricher converts validated events into records.
type Enricher struct {
	hasher *VisitorHasher
}

// New returns an Enricher that derives visitor hashes with hasher.
func New(hasher *VisitorHasher) *Enricher {
	return &Enricher{hasher: hasher}
}

// Enrich builds a Record from e, which must already have passed validation.
func (en *Enricher) Enrich(e *event.Event, meta RequestMeta) (event.Record, error) {
	page, err := url.Parse(e.URL)
	if err != nil {
		return event.Record{}, fmt.Errorf("parse url: %w", err)
	}
	host := strings.ToLower(page.Hostname())

	utm := NormalizeUTM(e.UTM).Merge(ParseUTM(e.URL))
	refHost := ReferrerHost(e.Referrer)

	visitor, err := en.hasher.Hash(meta.ReceivedAt, host, meta.IP, meta.UserAgent)
	if err != nil {
		return event.Record{}, err
	}

	ts := e.Timestamp
	if ts.IsZero() {
		ts = meta.ReceivedAt
	}
	id := e.MessageID
	if id == "" {
		id = rand.Text()
	}
	path := page.EscapedPath()
	if path == "" {
		path = "/"
	}

	return event.Record{
		MessageID:   id,
		Type:        e.Type,
		Name:        e.Name,
		AnonymousID: e.AnonymousID,
		VisitorHash: visitor,
		// The query string is dropped from the stored URL: it routinely
		// carries emails, tokens and other personal data, and the useful
		// parts (UTM tags, click IDs) have already been extracted.
		URL:          page.Scheme + "://" + page.Host + path,
		Host:         host,
		Path:         path,
		Referrer:     stripQuery(e.Referrer),
		ReferrerHost: refHost,
		Channel: ClassifyChannel(ChannelInput{
			ReferrerHost: refHost,
			PageHost:     host,
			UTM:          utm,
			PaidClick:    hasPaidClickID(e.URL),
		}),
		UTM:        utm,
		Device:     ClassifyDevice(meta.UserAgent),
		Properties: e.Properties,
		Timestamp:  ts.UTC(),
		ReceivedAt: meta.ReceivedAt.UTC(),
	}, nil
}

func stripQuery(raw string) string {
	raw, _, _ = strings.Cut(raw, "#")
	raw, _, _ = strings.Cut(raw, "?")
	return raw
}
