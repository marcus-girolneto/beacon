// Package event defines incoming events, enriched records and validation.
package event

import (
	"encoding/json"
	"time"
)

// Type is an event kind.
type Type string

// Event types.
const (
	TypePageview Type = "pageview"
	TypeClick    Type = "click"
	TypeCustom   Type = "custom"
)

// Valid reports whether t is a known type.
func (t Type) Valid() bool {
	switch t {
	case TypePageview, TypeClick, TypeCustom:
		return true
	default:
		return false
	}
}

// UTM holds campaign parameters.
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

// Merge fills empty fields of u from fallback.
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

// Event is an event as submitted by a client.
type Event struct {
	MessageID   string          `json:"message_id,omitempty"`
	Type        Type            `json:"type"`
	Name        string          `json:"name,omitempty"`
	AnonymousID string          `json:"anonymous_id"`
	URL         string          `json:"url"`
	Referrer    string          `json:"referrer,omitempty"`
	UTM         UTM             `json:"utm,omitzero"`
	Properties  json.RawMessage `json:"properties,omitempty"`
	Timestamp   time.Time       `json:"timestamp,omitzero"`
}

// Channel is an acquisition channel.
type Channel string

// Channels.
const (
	ChannelDirect   Channel = "direct"
	ChannelSearch   Channel = "search"
	ChannelSocial   Channel = "social"
	ChannelEmail    Channel = "email"
	ChannelPaid     Channel = "paid"
	ChannelReferral Channel = "referral"
)

// Device is a coarse device class.
type Device string

// Device classes.
const (
	DeviceDesktop Device = "desktop"
	DeviceMobile  Device = "mobile"
	DeviceTablet  Device = "tablet"
	DeviceBot     Device = "bot"
	DeviceUnknown Device = "unknown"
)

// Record is an enriched event. It never holds the IP or raw User-Agent.
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
