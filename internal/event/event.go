// Package event defines the analytics event model accepted at the edge, the
// enriched record that flows through the pipeline, and the validation rules
// applied to incoming events.
package event

import (
	"encoding/json"
	"time"
)

// Type is the kind of an analytics event.
type Type string

// Supported event types.
const (
	TypePageview Type = "pageview"
	TypeClick    Type = "click"
	TypeCustom   Type = "custom"
)

// Valid reports whether t is one of the supported event types.
func (t Type) Valid() bool {
	switch t {
	case TypePageview, TypeClick, TypeCustom:
		return true
	default:
		return false
	}
}

// UTM holds the standard campaign tracking parameters.
type UTM struct {
	Source   string `json:"source,omitempty"`
	Medium   string `json:"medium,omitempty"`
	Campaign string `json:"campaign,omitempty"`
	Term     string `json:"term,omitempty"`
	Content  string `json:"content,omitempty"`
}

// IsZero reports whether no UTM parameter is set.
func (u UTM) IsZero() bool {
	return u == UTM{}
}

// Merge returns u with every empty field filled from fallback. Values set
// explicitly by the client therefore win over values parsed from the URL.
func (u UTM) Merge(fallback UTM) UTM {
	if u.Source == "" {
		u.Source = fallback.Source
	}
	if u.Medium == "" {
		u.Medium = fallback.Medium
	}
	if u.Campaign == "" {
		u.Campaign = fallback.Campaign
	}
	if u.Term == "" {
		u.Term = fallback.Term
	}
	if u.Content == "" {
		u.Content = fallback.Content
	}
	return u
}

// Event is a single analytics event as submitted by a client.
type Event struct {
	// MessageID is an optional client-supplied idempotency key. The server
	// generates one when it is absent.
	MessageID   string          `json:"message_id,omitempty"`
	Type        Type            `json:"type"`
	Name        string          `json:"name,omitempty"`
	AnonymousID string          `json:"anonymous_id"`
	URL         string          `json:"url"`
	Referrer    string          `json:"referrer,omitempty"`
	UTM         UTM             `json:"utm,omitzero"`
	Properties  json.RawMessage `json:"properties,omitempty"`
	// Timestamp is when the event happened on the client. The server uses
	// the receive time when it is absent.
	Timestamp time.Time `json:"timestamp,omitzero"`
}

// Channel is the marketing acquisition channel an event is attributed to.
type Channel string

// Supported acquisition channels.
const (
	ChannelDirect   Channel = "direct"
	ChannelSearch   Channel = "search"
	ChannelSocial   Channel = "social"
	ChannelEmail    Channel = "email"
	ChannelPaid     Channel = "paid"
	ChannelReferral Channel = "referral"
)

// Device is a coarse device class derived from the User-Agent header.
type Device string

// Supported device classes.
const (
	DeviceDesktop Device = "desktop"
	DeviceMobile  Device = "mobile"
	DeviceTablet  Device = "tablet"
	DeviceBot     Device = "bot"
	DeviceUnknown Device = "unknown"
)

// Record is an enriched, privacy-safe event ready to be written to a sink.
// It never contains the client IP address or the raw User-Agent.
type Record struct {
	MessageID    string          `json:"message_id"`
	Type         Type            `json:"type"`
	Name         string          `json:"name,omitempty"`
	AnonymousID  string          `json:"anonymous_id"`
	VisitorHash  string          `json:"visitor_hash"`
	URL          string          `json:"url"`
	Host         string          `json:"host"`
	Path         string          `json:"path"`
	Referrer     string          `json:"referrer,omitempty"`
	ReferrerHost string          `json:"referrer_host,omitempty"`
	Channel      Channel         `json:"channel"`
	UTM          UTM             `json:"utm,omitzero"`
	Device       Device          `json:"device"`
	Properties   json.RawMessage `json:"properties,omitempty"`
	Timestamp    time.Time       `json:"timestamp"`
	ReceivedAt   time.Time       `json:"received_at"`
}
