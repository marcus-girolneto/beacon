// Package stats defines the aggregate query model served by the stats API.
package stats

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidQuery is matched by every error returned from Query.Validate.
var ErrInvalidQuery = errors.New("invalid stats query")

// Dimension is an attribute events can be grouped by.
type Dimension string

// Supported dimensions.
const (
	ByCampaign Dimension = "utm_campaign"
	BySource   Dimension = "utm_source"
	ByChannel  Dimension = "channel"
	ByPath     Dimension = "path"
	ByDevice   Dimension = "device"
	ByDay      Dimension = "day"
)

// Dimensions returns every supported dimension.
func Dimensions() []Dimension {
	return []Dimension{ByCampaign, BySource, ByChannel, ByPath, ByDevice, ByDay}
}

// ParseDimension converts s into a Dimension.
func ParseDimension(s string) (Dimension, error) {
	for _, d := range Dimensions() {
		if string(d) == s {
			return d, nil
		}
	}
	return "", fmt.Errorf("%w: unknown group_by %q", ErrInvalidQuery, s)
}

// Limits applied to queries.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
	MaxRange     = 366 * 24 * time.Hour
)

// Query selects events in the half-open interval [From, To) and groups them
// by GroupBy.
type Query struct {
	From    time.Time
	To      time.Time
	GroupBy Dimension
	// Host optionally restricts results to one site.
	Host string
	// Limit caps the number of groups returned.
	Limit int
}

// Validate checks q and returns an error wrapping ErrInvalidQuery if it is
// unusable.
func (q Query) Validate() error {
	switch {
	case q.From.IsZero() || q.To.IsZero():
		return fmt.Errorf("%w: from and to are required", ErrInvalidQuery)
	case !q.To.After(q.From):
		return fmt.Errorf("%w: to must be after from", ErrInvalidQuery)
	case q.To.Sub(q.From) > MaxRange:
		return fmt.Errorf("%w: range must not exceed %d days", ErrInvalidQuery, int(MaxRange.Hours()/24))
	case q.Limit < 1 || q.Limit > MaxLimit:
		return fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxLimit)
	}
	if _, err := ParseDimension(string(q.GroupBy)); err != nil {
		return err
	}
	return nil
}

// Row is the aggregate for one group.
type Row struct {
	Key       string `json:"key"`
	Events    int64  `json:"events"`
	Pageviews int64  `json:"pageviews"`
	// Visitors counts distinct daily visitor hashes. Because hashes rotate
	// every UTC day, a person active on three days counts three times in a
	// multi-day range; this is the price of not keeping linkable identifiers.
	Visitors int64 `json:"visitors"`
}

// Result is the response to a Query.
type Result struct {
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	GroupBy Dimension `json:"group_by"`
	Totals  Row       `json:"totals"`
	Rows    []Row     `json:"rows"`
}
