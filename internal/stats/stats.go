// Package stats defines the stats query model.
package stats

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidQuery matches every Query.Validate error.
var ErrInvalidQuery = errors.New("invalid stats query")

// Dimension is a group-by attribute.
type Dimension string

// Dimensions.
const (
	ByCampaign Dimension = "utm_campaign"
	BySource   Dimension = "utm_source"
	ByChannel  Dimension = "channel"
	ByPath     Dimension = "path"
	ByDevice   Dimension = "device"
	ByDay      Dimension = "day"
)

// Dimensions returns all dimensions.
func Dimensions() []Dimension {
	return []Dimension{ByCampaign, BySource, ByChannel, ByPath, ByDevice, ByDay}
}

// ParseDimension parses s.
func ParseDimension(s string) (Dimension, error) {
	for _, d := range Dimensions() {
		if string(d) == s {
			return d, nil
		}
	}
	return "", fmt.Errorf("%w: unknown group_by %q", ErrInvalidQuery, s)
}

// Query limits.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
	MaxRange     = 366 * 24 * time.Hour
)

// Query selects events in [From, To) grouped by GroupBy.
type Query struct {
	From    time.Time
	To      time.Time
	GroupBy Dimension
	Host    string // optional
	Limit   int
}

// Validate returns an error wrapping ErrInvalidQuery if q is unusable.
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
	Key       string `json:"key,omitempty"`
	Events    int64  `json:"events"`
	Pageviews int64  `json:"pageviews"`
	// daily unique, a visitor active on 3 days counts 3 times
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
